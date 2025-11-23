import { DuckDBInstance } from "@duckdb/node-api";
import fs from "fs";
import crypto from "crypto";
export default async function () {
  try {
    const meetingsFilePath = "src/data/meetings.parquet";
    const commissionsFilePath = "src/data/commissions.parquet";
    const votesFilePath = "src/data/votes.parquet";
    const questionsFilePath = "src/data/questions.parquet";
       const commissionQuestionsFilePath = "src/data/commission_questions.parquet";
    const propositionsFilePath = "src/data/propositions.parquet";
    const membersFilePath = "src/data/members.parquet";
    const summariesFilePath = "src/data/summaries.parquet";
    const dossiersFilePath = "src/data/dossiers.parquet";

    const allFilesExist = [
      meetingsFilePath,
      commissionsFilePath,
      votesFilePath,
      questionsFilePath,
      commissionQuestionsFilePath,
      propositionsFilePath,
      membersFilePath,
      summariesFilePath,
      dossiersFilePath,
    ].every(fs.existsSync);

    if (!allFilesExist) {
      console.error("Parquet file(s) not found.");
      return { meetings: [] };
    }

    const instance = await DuckDBInstance.create(":memory:");
    const connection = await instance.connect();

    const readParquet = async (filePath) => {
      const result = await connection.runAndReadAll(
        `SELECT * FROM read_parquet('${filePath}')`,
      );
      return result.getRows();
    };

    const [
      meetingsRows,
      commissionsRows,
      votesRows,
      questionsRows,
      commissionQuestionsRows,
      propositionsRows,
      membersRows,
      summariesRows,
      dossiersRows,
    ] = await Promise.all([
      readParquet(meetingsFilePath),
      readParquet(commissionsFilePath),
      readParquet(votesFilePath),
      readParquet(questionsFilePath),
        readParquet(commissionQuestionsFilePath),
      readParquet(propositionsFilePath),
      readParquet(membersFilePath),
      readParquet(summariesFilePath),
      readParquet(dossiersFilePath),
    ]);

    // LOOKUP TABLS
    const summaryByHash = {};
    summariesRows.forEach((row) => {
      summaryByHash[row[0]] = row[2]; // Assuming row[0] is input_hash and row[2] is summary
    });

    const dossierById = {};
    dossiersRows.forEach((row) => {
      // Adjust these indices as per your schema
      const id = row[1]; // dossier_id
      const authors = row[3]; // comma-separated string or array
      const document_type = row[7]; // e.g., "Proposal"
      const status = row[8]; // e.g., "Adopted"

      dossierById[id] = {
        authors,
        document_type,
        status,
      };
    });

    // Lookup party by member.
    const memberPartyLookup = {};
    membersRows.forEach((row) => {
      const memberId = row[2] + " " + row[3]; // Assuming member_id is at index 0
      const party = row[9]; // Assuming party name is at index 1
      memberPartyLookup[memberId] = party;
    });

    // all members
    const activeMembers = membersRows
        .filter(row => row[12] === "true")
        .map(row => {
          const name = row[2] + " " + row[3];
          return {
            name,
            party: row[9] || "Unknown"
          };
        });

    const meetingDateMap = new Map();
    meetingsRows.forEach((row) => {
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

    // BUILD COMBINED MEETINGS

    let meetings = {};

    // PLENARY MEETINGS
    meetingsRows.forEach((row) => {
      const sessionId = row[0];
      const meetingId = row[1];
      const time_of_day = row[3];

      if (!meetings[sessionId]) {
        meetings[sessionId] = [];
      }

      meetings[sessionId].push({
        type: "plenary",
        commission_type: null,
        session_id: sessionId,
        meeting_id: meetingId,
        date: row[2],
        start_time: row[4],
        end_time: row[5],
        time_of_day: time_of_day,
        questions: [],
        propositions: [],
        votes: [],
        allVotes: [],
        chair: "Peter De Roover"
      });
    });

    // COMMISSION MEETINGS
    commissionsRows.forEach((row) => {
      const [
        sessionId,
        commissionId,
        date,
        time_of_day,
        start_time,
        end_time,
        commissionName, // e.g. "Finance-Budget"
      ] = row;

      if (!meetings[sessionId]) meetings[sessionId] = [];

      meetings[sessionId].push({
        type: "commission", // NEW
        commission_type: commissionName, // NEW
        session_id: sessionId,
        meeting_id: commissionId, // treat commission_id as meeting_id
        date,
        start_time,
        end_time,
        time_of_day,
        // These stay empty for commissions
        questions: [],
        propositions: [],
        votes: [],
        allVotes: [],
        chairs: row[7]
                    ? row[7].split(',').map(chair => {
                        const name = chair.trim();
                        return {
                            name,
                            party: (memberPartyLookup[name] || "Unknown").trim()
                        };
                    })
                    : [],
      });
    });

    questionsRows.forEach((row) => {
      const sessionId = row[1];
      const meetingId = row[2];

      const keyForDate = `${sessionId}-${meetingId}`;
      const date = meetingDateMap.get(keyForDate) || null;

      // Find the meeting that corresponds to the session_id and meeting_id
      if (meetings[sessionId]) {
        const meeting = meetings[sessionId].find(
          (meeting) => meeting.meeting_id === meetingId && meeting.type == "plenary",
        );
        if (meeting) {
          const questioners = row[3].split(",").map((q) => {
            const name = q.trim();
            return {
              name: name,
              party: memberPartyLookup[name] || "Unknown", // Same logic as in votes
            };
          });

          const respondents = row[4].split(",").map((q) => {
            const name = q.trim();
            return {
              name: name,
              party: memberPartyLookup[name] || "Unknown",
            };
          });

          const topics_nl = row[5].split(";").map((t) => t.trim());
          const topics_fr = row[6].split(";").map((t) => t.trim());

          const rawTopicsNl = row[5];
          const hash = hashText(rawTopicsNl);

          const topics_summary_nl = summaryByHash[hash] || null;

          const discussion = JSON.parse(row[7]).map(discussionItem => ({
            speaker: {
              name: discussionItem.speaker,
              party: memberPartyLookup[discussionItem.speaker] || "Unknown"
            },
            text: discussionItem.text
          }));


          const discussion_ids = row[8].split(",").map((d) => d.trim());

          meeting.questions.push({
            type: 'plenary',
            question_id: row[0],
            session_id: sessionId,
            meeting_id: meetingId,
            date: date,
            questioners: questioners,
            respondents: respondents,
            topics_nl: topics_nl,
            topics_fr: topics_fr,
            topics_summary_nl: topics_summary_nl,
            topics_summary_fr: topics_summary_nl,
            discussion: discussion,
            discussion_ids: discussion_ids,
          });
        }
      }
    });

    commissionQuestionsRows.forEach((row) => {
      const sessionId = row[1];
      const meetingId = row[2];

      if (sessionId === '404') {
        return; // skip
      }

      const keyForDate = `${sessionId}-${meetingId}`;
      const date = commissionDateMap.get(keyForDate) || null;

      // Find the meeting that corresponds to the session_id and meeting_id
      if (meetings[sessionId]) {
        const meeting = meetings[sessionId].find(
          (meeting) => meeting.meeting_id === meetingId && meeting.type == "commission",
        );
        if (meeting) {
          const questioners = row[3].split(",").map((q) => {
            const name = q.trim();
            return {
              name: name,
              party: memberPartyLookup[name] || "Unknown", // Same logic as in votes
            };
          });

          const respondents = row[4].split(",").map((q) => {
            const name = q.trim();
            return {
              name: name,
              party: memberPartyLookup[name] || "Unknown",
            };
          });

          const topics_nl = row[5].split(";").map((t) => t.trim());
          const topics_fr = row[6].split(";").map((t) => t.trim());

          const rawTopicsNl = row[5];
          const hash = hashText(rawTopicsNl);

          const topics_summary_nl = summaryByHash[hash] || null;

          const discussion = JSON.parse(row[7]).map(discussionItem => ({
            speaker: {
              name: discussionItem.speaker,
              party: memberPartyLookup[discussionItem.speaker] || "Unknown"
            },
            text: discussionItem.text
          }));


          const discussion_ids = row[8].split(",").map((d) => d.trim());

          meeting.questions.push({
            type: 'commission',
            question_id: row[0],
            session_id: sessionId,
            meeting_id: meetingId,
            date: date,
            questioners: questioners,
            respondents: respondents,
            topics_nl: topics_nl,
            topics_fr: topics_fr,
            topics_summary_nl: topics_summary_nl,
            topics_summary_fr: topics_summary_nl,
            discussion: discussion,
            discussion_ids: discussion_ids
          });
        }
      }
    });

    // Go through propositions.
    propositionsRows.forEach((row) => {
      const sessionId = row[1];
      const meetingId = row[2];
      const title_nl = row[3];
      const title_fr = row[4];
      const dossier_id = row[5];

      const rawTitleNl = row[3];
      const hash = hashText(rawTitleNl + ".");
      const title_summary_nl = summaryByHash[hash] || null;

      const dossierData = dossierById[dossier_id] || {};

      const authors =
        dossierData.authors?.split(",").map((q) => {
          const name = q.trim();
          return {
            name: name,
            party: memberPartyLookup[name] || "Unknown", // Same logic as in votes
          };
        }) || [];

      if (meetings[sessionId]) {
        const meeting = meetings[sessionId].find(
          (meeting) => meeting.meeting_id === meetingId,
        );
        if (meeting) {
          meeting.propositions.push({
            proposition_id: row[0],
            session_id: sessionId,
            meeting_id: meetingId,
            title_nl: title_nl,
            title_fr: title_fr,
            title_summary_nl: title_summary_nl,
            dossier_id: dossier_id,
            authors: authors,
            document_type: dossierData.document_type || null,
            status: dossierData.status || null,
          });
        }
      }
    });

    // Find proposition by session + dossier.
    const propositionMap = new Map();
    Object.values(meetings)
      .flat()
      .forEach((meeting) => {
        meeting.propositions.forEach((prop) => {
          if (prop.dossier_id) {
            const key = `${prop.session_id}-${prop.dossier_id}`;
            propositionMap.set(key, prop);
            prop.votes = [];
          }
        });
      });

    votesRows.forEach((row) => {
      const sessionId = row[1];
      const meetingId = row[2];
      const title_nl = row[4];
      const title_fr = row[5];

      const yesMembers = row[9]
        ? row[9].split(",").map((member) => member.trim())
        : [];
      const noMembers = row[10]
        ? row[10].split(",").map((member) => member.trim())
        : [];
      const abstainMembers = row[11]
        ? row[11].split(",").map((member) => member.trim())
        : [];

      const yesMembersWithParty = yesMembers.map((member) => ({
        name: member,
        party: memberPartyLookup[member] || "Unknown", // Default to "Unknown" if no party found
      }));
      const noMembersWithParty = noMembers.map((member) => ({
        name: member,
        party: memberPartyLookup[member] || "Unknown",
      }));
      const abstainMembersWithParty = abstainMembers.map((member) => ({
        name: member,
        party: memberPartyLookup[member] || "Unknown",
      }));

      const keyForDate = `${sessionId}-${meetingId}`;
      const date = meetingDateMap.get(keyForDate) || null;

      // Build detailed party grouping
      const groupedVotesByParty = {};
      const voteTypes = [
        ["yes", yesMembersWithParty],
        ["no", noMembersWithParty],
        ["abstain", abstainMembersWithParty],
      ];

      voteTypes.forEach(([type, members]) => {
        members.forEach((member) => {
          const party = member.party || "Unknown";

          // Init structure if needed
          if (!groupedVotesByParty[party]) {
            groupedVotesByParty[party] = { yes: [], no: [], abstain: [] };
          }

          groupedVotesByParty[party][type].push(member);
        });
      });

      // Optional: still keep vote counts if needed
      const partyVoteCounts = {};
      for (const party in groupedVotesByParty) {
        partyVoteCounts[party] = {
          yes: groupedVotesByParty[party].yes.length,
          no: groupedVotesByParty[party].no.length,
          abstain: groupedVotesByParty[party].abstain.length,
        };
      }

      // Find the meeting that corresponds to the session_id and meeting_id
      if (meetings[sessionId]) {
        const meeting = meetings[sessionId].find(
          (meeting) => meeting.meeting_id === meetingId,
        );
        if (meeting) {
          const vote = {
            vote_id: row[0],
            session_id: sessionId,
            meeting_id: meetingId,
            date: date,
            title_nl: title_nl,
            title_fr: title_fr,
            yes_count: row[6],
            no_count: row[7],
            abstain_count: row[8],
            yes_members: yesMembersWithParty,
            no_members: noMembersWithParty,
            abstain_members: abstainMembersWithParty,
            votes_by_party: partyVoteCounts,
            grouped_votes_by_party: groupedVotesByParty,
            dossier_id: row[12],
            document_id: row[13],
          };

          // Add vote to proposition if it matches by session and dossier
          const propKey = `${sessionId}-${vote.dossier_id}`;
          if (propositionMap.has(propKey)) {
            propositionMap.get(propKey).votes.push(vote);
            meeting.allVotes.push(vote); // collection of all votes
          } else {
            // If no matching proposition, add it directly to the meeting
            meeting.votes.push(vote);
            meeting.allVotes.push(vote); // collection of all votes
          }
        }
      }
    });

    await connection.close();

    const timeOfDayOrder = {
      evening: 0,
      afternoon: 1,
      morning: 2,
    };

    let allMeetings = Object.keys(meetings).flatMap((sessionId) =>
      meetings[sessionId].map((meeting) => ({
        ...meeting,
        session_id: sessionId,
        meeting_id: meeting.meeting_id,
      })),
    );

    // Sort by date descending, then by custom time_of_day order
    allMeetings = allMeetings.sort((a, b) => {
      const dateA = new Date(a.date);
      const dateB = new Date(b.date);

      if (dateA.getTime() !== dateB.getTime()) {
        return dateB - dateA; // Most recent date first
      }

      const timeA = timeOfDayOrder[a.time_of_day] ?? 999;
      const timeB = timeOfDayOrder[b.time_of_day] ?? 999;

      return timeA - timeB; // avond (0) comes before voormiddag (2)
    });

    const durations = allMeetings.map((meeting) => {
      // Fix the time format from '14h19' to '14:19'
      const startTime = meeting.start_time.replace("h", ":");
      const endTime = meeting.end_time.replace("h", ":");

      const start = new Date(`1970-01-01T${startTime}`);
      const end = new Date(`1970-01-01T${endTime}`);

      // Duration in minutes
      let duration = (end - start) / 60000;

      // Fix for overnight meetings
      if (duration < 0) {
        duration += 1440; // add 24 hours
      }

      return duration;
    });

    // Calculate attendance.
    // Add attendance info to each meeting
    allMeetings.forEach((meeting) => {
      if (meeting.allVotes.length > 0) {
        const firstVote = meeting.allVotes[0];
        const yes = parseInt(firstVote.yes_count, 10) || 0;
        const no = parseInt(firstVote.no_count, 10) || 0;
        const abstain = parseInt(firstVote.abstain_count, 10) || 0;
        const total = yes + no + abstain;
        const attendanceRatio = total / 150;

        const presentMembers = [
          ...firstVote.yes_members,
          ...firstVote.no_members,
          ...firstVote.abstain_members
        ].map(m => m.name);
        
        meeting.absentees = activeMembers
            .filter(m => !presentMembers.includes(m.name))
            .sort((a, b) => a.party.localeCompare(b.party));

        meeting.attendance = {
          count: total,
          ratio: attendanceRatio,
        };
      } else {
        meeting.attendance = {
          count: 0,
          ratio: 0,
        };
        meeting.absentees = [];
      }
    });

    return { meetings: allMeetings, durations: durations };
  } catch (error) {
    console.error("Error reading Parquet file:", error);
    return { meetings: [] };
  }
}

const hashText = (text) => {
  return crypto.createHash("sha256").update(text).digest("hex");
};
