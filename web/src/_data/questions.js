import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';
import crypto from 'crypto';

export default async function () {
    try {
        const questionsFilePath = 'src/data/questions.parquet';
         const commissionQuestionsFilePath = 'src/data/commission_questions.parquet';
        const meetingsFilePath = 'src/data/meetings.parquet';
           const commissionsFilePath = "src/data/commissions.parquet";
        const membersFilePath = 'src/data/members.parquet';
        const summariesFilePath = 'src/data/summaries.parquet';

        const allFilesExist = [
            questionsFilePath,
            commissionQuestionsFilePath,
            meetingsFilePath,
            commissionsFilePath,
            membersFilePath,
            summariesFilePath,
        ].every(fs.existsSync);

        if (!allFilesExist) {
            console.error('Parquet file(s) not found.');
            return { meetings: [] };
        }

        const instance = await DuckDBInstance.create(':memory:');
        const connection = await instance.connect();

        const readParquet = async (filePath) => {
            const result = await connection.runAndReadAll(`SELECT * FROM read_parquet('${filePath}')`);
            return result.getRows();
        };

        function safeParseDiscussion(raw, questionId) {
  try {
    // If DuckDB already returned a JS object/array, keep it
    const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;

    if (Array.isArray(parsed)) {
      return parsed;            // ✔️ happy path
    }

    // If we get here the JSON was valid but not an array
    console.warn(`question ${questionId}: discussion is not an array, skipping`);
    return [];
  } catch (e) {
    console.warn(`question ${questionId}: cannot parse discussion – ${e.message}`);
    return [];
  }
}


        const [
            questionsRows,
              commissionQuestionsRows,
            meetingsRows,
            commissionsRows,
            membersRows,
            summariesRows,
        ] = await Promise.all([
            readParquet(questionsFilePath),
             readParquet(commissionQuestionsFilePath),
            readParquet(meetingsFilePath),
               readParquet(commissionsFilePath),
            readParquet(membersFilePath),
            readParquet(summariesFilePath),
        ]);

            // Build summary lookup.
    const summaryByHash = {};
    summariesRows.forEach(row => {
      summaryByHash[row[0]] = row[2]; // Assuming row[0] is input_hash and row[2] is summary
    });


        const meetingDateMap = new Map();
        meetingsRows.forEach(row => {
            const sessionId = row[0];
            const meetingId = row[1];
            const date = row[2];
            const key = `${sessionId}-${meetingId}`;
            meetingDateMap.set(key, date);
        });

      const commissionDateMap = new Map();
        commissionsRows.forEach((row) => {
          const sessionId = row[0];
          const meetingId = row[1];
          const date = row[2];
          const key = `${sessionId}-${meetingId}`;
          commissionDateMap.set(key, date);
        });

        const memberPartyLookup = {};
        membersRows.forEach(row => {
          const memberId = row[2] + " " + row[3];  // Assuming member_id is at index 0
          const party = row[9];     // Assuming party name is at index 1
          memberPartyLookup[memberId] = party;
        });


        // BUILD COMBINED QUESTIONS
        let questions = [];

        // PLENARY QUESTIONS
        questionsRows.forEach(row => {
            const sessionId = row[1];
            const meetingId = row[2];

            const keyForDate = `${sessionId}-${meetingId}`;
            const date = meetingDateMap.get(keyForDate) || null;


            const discussion = JSON.parse(row[7]).map(discussionItem => ({
                speaker: {
                    name: discussionItem.speaker,
                    party: memberPartyLookup[discussionItem.speaker] || "Unknown"
                  },
                text: discussionItem.text
            }));

            const topics_nl = row[5].split(";").map(t => t.trim());
            const topics_fr = row[6].split(";").map(t => t.trim());

            const rawTopicsNl = row[5];
            const hash = hashText(rawTopicsNl);
            const topics_summary_nl = summaryByHash[hash] || null;

            const questioners = row[3].split(",").map(q => {
                const name = q.trim();
                return {
                  name: name,
                  party: memberPartyLookup[name] || "Unknown"
                };
              });

            const respondents = row[4].split(",").map(q => {
                const name = q.trim();
                return {
                  name: name,
                  party: memberPartyLookup[name] || "Unknown"
                };
              });

            const question = {
                type: "plenary",
                question_id: row[0],
                session_id: row[1],
                meeting_id: row[2],
                date: date,
                topics_nl: topics_nl,
                topics_fr: topics_fr,
                topics_summary_nl: topics_summary_nl,
                topics_summary_fr: topics_summary_nl,
                questioners: questioners,
                respondents: respondents,
                discussion: discussion
            };

            questions.push(question);
        });

        // COMMISSION QUESTIONS
         commissionQuestionsRows.forEach(row => {
            const sessionId = row[1];
            const meetingId = row[2];

            if (sessionId === '404') {
              return; // skip
            }

            const keyForDate = `${sessionId}-${meetingId}`;
            const date = commissionDateMap.get(keyForDate) || null;

            const discussion = JSON.parse(row[7]).map(discussionItem => ({
                speaker: {
                    name: discussionItem.speaker,
                    party: memberPartyLookup[discussionItem.speaker] || "Unknown"
                  },
                text: discussionItem.text
            }));


            const topics_nl = row[5].split(";").map(t => t.trim());
            const topics_fr = row[6].split(";").map(t => t.trim());

            const rawTopicsNl = row[5];
            const hash = hashText(rawTopicsNl);
            const topics_summary_nl = summaryByHash[hash] || null;

            const questioners = row[3].split(",").map(q => {
                const name = q.trim();
                return {
                  name: name,
                  party: memberPartyLookup[name] || "Unknown"
                };
              });

            const respondents = row[4].split(",").map(q => {
                const name = q.trim();
                return {
                  name: name,
                  party: memberPartyLookup[name] || "Unknown"
                };
              });

            const question = {
                type: "commission",
                question_id: row[0],
                session_id: row[1],
                meeting_id: row[2],
                date: date,
                topics_nl: topics_nl,
                topics_fr: topics_fr,
                topics_summary_nl: topics_summary_nl,
                topics_summary_fr: topics_summary_nl,
                questioners: questioners,
                respondents: respondents,
                discussion: discussion
            };

            questions.push(question);
        });

        await connection.close();

        return { questions: questions };
    } catch (error) {
        console.error('Error reading Parquet file:', error);
        return { questions: [] };
    }
}


const hashText = (text) => {
  return crypto.createHash('sha256').update(text).digest('hex');
};