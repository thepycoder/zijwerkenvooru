import { DuckDBInstance } from "@duckdb/node-api";
import fs from "fs";

import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const partyColorsPath = path.join(__dirname, "partyColors.json");
const partyColors = JSON.parse(fs.readFileSync(partyColorsPath, "utf-8"));

const topicsPath = path.join(__dirname, "topics.json");
const topics = JSON.parse(fs.readFileSync(topicsPath, "utf-8"));

export default async function () {
  try {
    const membersFilePath = "src/data/members.parquet";
    const remunerationsFilePath = "src/data/remunerations.parquet";
    const questionsFilePath = "src/data/questions.parquet";
    const commissionQuestionsFilePath = "src/data/commission_questions.parquet";
    const meetingsFilePath = "src/data/meetings.parquet";
    const commissionsFilePath = "src/data/commissions.parquet";
    const votesFilePath = "src/data/votes.parquet";
    const propositionsFilePath = "src/data/propositions.parquet";
    const dossiersFilePath = "src/data/dossiers.parquet";
    const subdocumentsFilePath = "src/data/subdocuments.parquet";

    if (
      !fs.existsSync(membersFilePath) ||
      !fs.existsSync(remunerationsFilePath) ||
      !fs.existsSync(questionsFilePath) ||
      !fs.existsSync(commissionQuestionsFilePath) ||
      !fs.existsSync(meetingsFilePath) ||
      !fs.existsSync(commissionsFilePath) ||
      !fs.existsSync(propositionsFilePath) ||
      !fs.existsSync(dossiersFilePath) ||
      !fs.existsSync(votesFilePath) ||
      !fs.existsSync(subdocumentsFilePath)
    ) {
      console.error("Parquet file(s) not found.");
      return { memberCount: 0, members: [], ages: [], incomes2023: [] };
    }

    const instance = await DuckDBInstance.create(":memory:");
    const connection = await instance.connect();

    const membersResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${membersFilePath}')`,
    );
    const membersRows = membersResult.getRows();

    const remunerationsResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${remunerationsFilePath}')`,
    );
    const remunerationsRows = remunerationsResult.getRows();

    const questionsResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${questionsFilePath}')`,
    );
    const questionsRows = questionsResult.getRows();

    const commissionQuestionsResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${commissionQuestionsFilePath}')`,
    );
    const commissionQuestionsRows = commissionQuestionsResult.getRows();

    const meetingsResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${meetingsFilePath}')`,
    );
    const meetingsRows = meetingsResult.getRows();

    const commissionsResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${commissionsFilePath}')`,
    );
    const commissionsRows = commissionsResult.getRows();

    const votesResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${votesFilePath}')`,
    );
    const votesRows = votesResult.getRows();

    const propositionsResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${propositionsFilePath}')`,
    );
    const propositionsRows = propositionsResult.getRows();

    const dossiersResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${dossiersFilePath}')`,
    );
    const dossiersRows = dossiersResult.getRows();

    const subdocumentsResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${subdocumentsFilePath}')`,
    );
    const subdocumentsRows = subdocumentsResult.getRows();

    /* Date map for plenary meetings. */
    const meetingDateMap = new Map();
    meetingsRows.forEach((row) => {
      const sessionId = row[0];
      const meetingId = row[1];
      const date = row[2];
      const key = `${sessionId}-${meetingId}`;
      meetingDateMap.set(key, date);
    });

    /* Date map for commission meetings. */
    const commissionDateMap = new Map();
    commissionsRows.forEach((row) => {
      const sessionId = row[0];
      const meetingId = row[1];
      const date = row[2];
      const key = `${sessionId}-${meetingId}`;
      commissionDateMap.set(key, date);
    });

    const memberMap = new Map();
    const currentDate = new Date(); // Get the current date for age calculation

    const convertDate = (rawDate) => {
      if (!rawDate || typeof rawDate !== "string") return null;
      const [day, month, year] = rawDate.split("/");
      if (!day || !month || !year) return null;
      return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
    };

    const dossierById = {};
    const dossierMap = new Map();
    dossiersRows.forEach((dossier) => {
      const sessionId = dossier[0];
      const dossierId = dossier[1];
      const id = dossier[1];
      const title = dossier[2];
      const status = dossier[8];
      const document_type = dossier[7];
      const vote_date = convertDate(dossier[6]);
      const authors = dossier[3]
        ?.split(",")
        .map((a) => a.trim().toLowerCase().replace(" ", "-")) || [];
      dossierMap.set(dossierId, { sessionId, title, authors });

      dossierById[id] = {
        authors,
        document_type,
        status,
        vote_date,
      };
    });

    // LOAD MEMBERS
    membersRows.forEach((row) => {
      const firstName = row[2];
      const lastName = row[3];
      const key = `${firstName}-${lastName}`.toLowerCase();

      if (!memberMap.has(key)) {
        const birthDate = new Date(row[5]);
        let age = null;
        if (birthDate instanceof Date && !isNaN(birthDate)) {
          age = currentDate.getFullYear() - birthDate.getFullYear(); // Start by calculating the year difference
          const month = currentDate.getMonth() - birthDate.getMonth(); // Check if the current month is before the birth month
          const d = currentDate.getDate() - birthDate.getDate(); // Check if the current day is before the birth day

          // Adjust the age if the current date is before the birthday in this year
          if (month < 0 || (month === 0 && d < 0)) {
            age -= 1;
          }
        }

        memberMap.set(key, {
          member_id: row[0],
          first_name: firstName,
          last_name: lastName,
          gender: row[4],
          date_of_birth: row[5],
          place_of_birth: row[6],
          language: row[7],
          constituency: row[8],
          sessions: new Set([row[1]]),
          parties: new Set([row[9]]),
          fractions: new Set([row[10]]),
          email: row[11],
          active: row[12],
          start_date: row[13],
          remunerations: {},
          propositions: [],
          questions: [],
          commissionQuestions: [],
          votes: [],
          age: age,
        });
      } else {
        const member = memberMap.get(key);
        member.sessions.add(row[1]);
        member.parties.add(row[9]);
        member.fractions.add(row[10]);
        if (member.language == null) member.language = row[6];
      }
    });

    // ADD PROPOSITIONS TO MEMBERS
    propositionsRows.forEach((prop) => {
      const propId = prop[0];
      const sessionId = prop[1];
      const meetingId = prop[2];
      const titleNl = prop[3];
      const titleFr = prop[4];
      const dossierId = prop[5];
      const documentId = prop[6];

      const dossierData = dossierById[dossierId] || {};

      const dossier = dossierMap.get(dossierId);
      if (!dossier) return;

      const authors = dossier.authors;

      authors.forEach((authorKey) => {
        if (memberMap.has(authorKey)) {
          const member = memberMap.get(authorKey);
          if (!member.propositions) member.propositions = [];

          member.propositions.push({
            proposition_id: propId,
            session_id: sessionId,
            meeting_id: meetingId,
            title_nl: titleNl,
            title_fr: titleFr,
            dossier_id: dossierId,
            document_id: documentId,
            dossier_title: dossier.title,
            document_type: dossierData.document_type || null,
            status: dossierData.status || null,
            vote_date: dossierData.vote_date,
          });
        }
      });
    });

    // ADD SUBDOCUMENTS TO MEMBERS
    subdocumentsRows.forEach((subdoc) => {
      const authorsRaw = subdoc[4]; // Adjust this index if needed
      const authors = authorsRaw
        .split(",")
        .map((name) => name.trim().toLowerCase().replace(/\s+/g, "-"));

      //console.log(authors);
      authors.forEach((authorKey) => {
        if (memberMap.has(authorKey)) {
          const member = memberMap.get(authorKey);
          if (!member.subdocuments) member.subdocuments = [];

          member.subdocuments.push({
            date: subdoc[2],
            type: subdoc[3],
          });
        }
      });
    });

    // ADD REMUNERATIONS TO MEMBERS
    remunerationsRows.forEach((remuneration) => {
      const firstName = remuneration[0];
      const lastName = remuneration[1];
      const key = `${firstName}-${lastName}`.toLowerCase();

      if (memberMap.has(key)) {
        const member = memberMap.get(key);
        const year = remuneration[2];

        if (!member.remunerations[year]) {
          member.remunerations[year] = {
            entries: [],
            total_min: 0,
            total_max: 0,
          };
        }
        const entry = {
          mandate: remuneration[3],
          institute: remuneration[4],
          remuneration_min: parseFloat(remuneration[5]) || 0,
          remuneration_max: parseFloat(remuneration[6]) || 0,
        };

        member.remunerations[year].entries.push(entry);
        member.remunerations[year].total_min += entry.remuneration_min;
        member.remunerations[year].total_max += entry.remuneration_max;
      }
    });

    const memberPartyLookup = {};
    membersRows.forEach((row) => {
      const memberId = row[2] + " " + row[3]; // Assuming member_id is at index 0
      const party = row[9]; // Assuming party name is at index 1
      memberPartyLookup[memberId] = party;
    });

    /* Go through plenary questions. */
    questionsRows.forEach((question) => {
      const questionId = question[0];
      const sessionId = question[1];
      const meetingId = question[2];
      const keyForDate = `${sessionId}-${meetingId}`;
      const date = meetingDateMap.get(keyForDate) || null;

      const questioners = question[3].split(",").map((q) => {
        const name = q.trim();
        return {
          name: name,
          party: memberPartyLookup[name] || "Unknown",
        };
      });

      const respondents = question[4].split(",").map((q) => {
        const name = q.trim();
        return {
          name: name,
          party: memberPartyLookup[name] || "Unknown",
        };
      });

      const topics_nl = question[5].split(";").map((topic) => topic.trim());
      const topics_fr = question[6].split(";").map((topic) => topic.trim());

      const discussion = JSON.parse(question[7]).map((discussionItem) => ({
        speaker: discussionItem.speaker,
        text: discussionItem.text,
      }));

      // Add to questioners
      questioners.forEach((q, i) => {
        const key = q.name.toLowerCase().replace(" ", "-");
        const topic_nl = topics_nl[i] || null;
        const topic_fr = topics_fr[i] || null;

        if (memberMap.has(key)) {
          const member = memberMap.get(key);
          member.questions.push({
            question_id: questionId,
            session_id: sessionId,
            meeting_id: meetingId,
            date: date,
            type: "plenary",
            topic_nl,
            topic_fr,
            topics_nl: [topic_nl],
            topics_fr: [topic_fr],
            questioners,
            respondents,
            discussion,
            asRespondent: false,
          });
        }
      });

      // Add to respondents
      respondents.forEach((r) => {
        const key = r.name.toLowerCase().replace(" ", "-");
        const topic_nl = topics_nl[0] || null;
        const topic_fr = topics_fr[0] || null;

        if (memberMap.has(key)) {
          const member = memberMap.get(key);
          member.questions.push({
            question_id: questionId,
            session_id: sessionId,
            meeting_id: meetingId,
            date: date,
            type: "plenary",
            topic_nl,
            topic_fr,
            topics_nl: [topic_nl],
            topics_fr: [topic_fr],
            questioners,
            respondents,
            discussion,
            asRespondent: true,
          });
        }
      });
    });

    /* Go through commission questions. */
    commissionQuestionsRows.forEach((question) => {
      const questionId = question[0];
      if (questionId === "404") {
        // TODO: why 404s?
        return;
      }
      const sessionId = question[1];
      const meetingId = question[2];
      const keyForDate = `${sessionId}-${meetingId}`;
      const date = commissionDateMap.get(keyForDate) || null;

      const questioners = question[3].split(",").map((q) => {
        const name = q.trim();
        return {
          name: name,
          party: memberPartyLookup[name] || "Unknown",
        };
      });

      const respondents = question[4].split(",").map((q) => {
        const name = q.trim();
        return {
          name: name,
          party: memberPartyLookup[name] || "Unknown",
        };
      });

      const topics_nl = question[5].split(";").map((topic) => topic.trim());
      const topics_fr = question[6].split(";").map((topic) => topic.trim());

      const discussion = JSON.parse(question[7]).map((discussionItem) => ({
        speaker: discussionItem.speaker,
        text: discussionItem.text,
      }));

      // Add to questioners
      questioners.forEach((q, i) => {
        const key = q.name.toLowerCase().replace(" ", "-");
        const topic_nl = topics_nl[i] || null;
        const topic_fr = topics_fr[i] || null;

        if (memberMap.has(key)) {
          const member = memberMap.get(key);
          member.commissionQuestions.push({
            question_id: questionId,
            session_id: sessionId,
            meeting_id: meetingId,
            date: date,
            type: "commission"
              .topic_nl,
            topic_fr,
            topics_nl: [topic_nl],
            topics_fr: [topic_fr],
            questioners,
            respondents,
            discussion,
            asRespondent: false,
          });
        }
      });

      // Add to respondents
      respondents.forEach((r) => {
        const key = r.name.toLowerCase().replace(" ", "-");
        const topic_nl = topics_nl[0] || null;
        const topic_fr = topics_fr[0] || null;

        if (memberMap.has(key)) {
          const member = memberMap.get(key);
          member.commissionQuestions.push({
            question_id: questionId,
            session_id: sessionId,
            meeting_id: meetingId,
            date: date,
            type: "commission"
              .topic_nl,
            topic_fr,
            topics_nl: [topic_nl],
            topics_fr: [topic_fr],
            questioners,
            respondents,
            discussion,
            asRespondent: true,
          });
        }
      });
    });

    // Handling votes
    votesRows.forEach((vote) => {
      const membersYes = vote[9]
        .split(",")
        .map((name) => name.trim().toLowerCase().replace(" ", "-"));
      const membersNo = vote[10]
        .split(",")
        .map((name) => name.trim().toLowerCase().replace(" ", "-"));
      const membersAbstain = vote[11]
        .split(",")
        .map((name) => name.trim().toLowerCase().replace(" ", "-"));

      // Group members by their party
      const partyVotes = new Map();

      [...membersYes, ...membersNo, ...membersAbstain].forEach((memberKey) => {
        const party = memberMap.get(memberKey)?.parties?.values().next().value; // Assuming member has a single party

        if (party) {
          if (!partyVotes.has(party)) {
            partyVotes.set(party, { yes: 0, no: 0, abstain: 0 });
          }
          const voteType = membersYes.includes(memberKey)
            ? "yes"
            : membersNo.includes(memberKey)
            ? "no"
            : "abstain";
          partyVotes.get(party)[voteType]++;
        }
      });

      // Determine the majority vote for each party
      const majorityVoteByParty = new Map();
      partyVotes.forEach((voteCount, party) => {
        let majorityVote = "abstain";
        if (voteCount.yes > voteCount.no && voteCount.yes > voteCount.abstain) {
          majorityVote = "yes";
        } else if (
          voteCount.no > voteCount.yes &&
          voteCount.no > voteCount.abstain
        ) {
          majorityVote = "no";
        }
        majorityVoteByParty.set(party, majorityVote);
      });

      // Check for outliers
      [...membersYes, ...membersNo, ...membersAbstain].forEach((memberKey) => {
        if (memberMap.has(memberKey)) {
          const member = memberMap.get(memberKey);
          const party = member.parties.values().next().value;
          const majorityVote = majorityVoteByParty.get(party);

          let voteType = "abstain";
          if (membersYes.includes(memberKey)) voteType = "yes";
          else if (membersNo.includes(memberKey)) voteType = "no";

          const isOutlier = voteType !== majorityVote;

          member.votes.push({
            vote_id: vote[0],
            session_id: vote[1],
            meeting_id: vote[2],
            date: vote[3],
            title_nl: vote[4],
            title_fr: vote[5],
            vote: voteType,
            outlier: isOutlier,
          });
        }
      });
    });

    const members = Array.from(memberMap.values()).map((member) => ({
      ...member,
      sessions: Array.from(member.sessions),
      parties: Array.from(member.parties),
      fractions: Array.from(member.fractions),
      votes: member.votes || [],
    }));

    // Parties.
    // After members array creation
    const partyStats = new Map();

    members.forEach((member) => {
      if (member.active === "false") return;
      member.parties.forEach((party) => {
        if (!partyStats.has(party)) {
          partyStats.set(party, {
            seats: 0,
            color: partyColors[party.toLowerCase()].primary || "gray",
          });
        }
        partyStats.get(party).seats++;
      });
    });

    const parties = Array.from(partyStats.entries())
      .map(([name, data]) => ({
        name,
        ...data,
      }))
      .sort((a, b) => b.seats - a.seats); // Sort by seats in descending order

    const ages = members.map((member) => member.age);
    const memberCount = membersRows.length;

    const incomes2023 = members.map((m) => {
      const r2023 = m.remunerations?.["2023"];
      if (!r2023) return 0; // or skip the member if you prefer
      const min = r2023.total_min || 0;
      const max = r2023.total_max || 0;
      return (min + max) / 2; // use max or min instead if you like
    });

    await connection.close();

    // Create the ages array (using the already calculated member data)
    const voteToNum = { yes: 1, no: -1, abstain: 0 };

    // Filter only active members
    const activeMembers = members.filter((m) => m.active !== "false");

    // Collect all unique vote titles
    const allVoteIds = Array.from(
      new Set(activeMembers.flatMap((m) => m.votes.map((v) => v.title))),
    );

    // Create a mapping from vote title to index
    const voteIndex = new Map(allVoteIds.map((id, i) => [id, i]));

    // Convert each member's votes into a numeric vector
    const memberVectors = activeMembers.map((m) => {
      const vec = Array(allVoteIds.length).fill(null);
      for (const v of m.votes) {
        const idx = voteIndex.get(v.title);
        if (idx !== undefined) vec[idx] = voteToNum[v.vote];
      }
      return vec;
    });

    // Helper to calculate cosine similarity
    function cosineSimilarity(a, b) {
      let dot = 0,
        magA = 0,
        magB = 0;
      for (let i = 0; i < a.length; i++) {
        if (a[i] !== null && b[i] !== null) {
          dot += a[i] * b[i];
          magA += a[i] * a[i];
          magB += b[i] * b[i];
        }
      }
      return dot / (Math.sqrt(magA) * Math.sqrt(magB) || 1);
    }

    // Build nodes with basic info
    const nodes = activeMembers.map((m) => ({
      id: m.member_id,
      name: `${m.first_name} ${m.last_name}`,
      party: m.parties[0],
      color: partyColors[m.parties[0]?.toLowerCase()]?.primary || "gray",
    }));

    // Group members by party once to avoid repeated filters
    const membersByParty = new Map();
    for (const [i, member] of activeMembers.entries()) {
      const party = member.parties[0];
      if (!membersByParty.has(party)) membersByParty.set(party, []);
      membersByParty.get(party).push({ member, index: i });
    }

    // Create links only within parties, using high similarity threshold
    const partyLinks = [];
    for (let i = 0; i < activeMembers.length; i++) {
      for (let j = i + 1; j < activeMembers.length; j++) {
        const vecA = memberVectors[i];
        const vecB = memberVectors[j];
        const sim = cosineSimilarity(vecA, vecB);

        if (sim > 0.9) {
          partyLinks.push({
            source: activeMembers[i].member_id,
            target: activeMembers[j].member_id,
            weight: sim,
          });
        }
      }
    }

    // CALCULATE TOP CONTRIBUTORS
    // Map to track counts: topic -> { memberKey -> { questions: count, propositions: count } }
    const topicMemberCounts = new Map();

    // First, prepare a flat list of all topics and subtopics
    const allTopics = {}; // { topicKey -> { type: "main"|"subtopic", parent: "parentKey" or null } }

    for (const [topicKey, data] of Object.entries(topics)) {
      allTopics[topicKey] = { type: "main", parent: null };

      if (data.subtopics) {
        for (const subtopicKey of Object.keys(data.subtopics)) {
          allTopics[subtopicKey] = { type: "subtopic", parent: topicKey };
        }
      }
    }

    members.forEach((member) => {
      const fullName = `${member.first_name} ${member.last_name}`;
      const party = `${member.parties[0]}`;

      const countContribution = (items, type) => {
        if (!items) return;

        items.forEach((item) => {
          const title = (item.topic_nl || item.title_nl || "").toLowerCase();

          for (const [topicKey, data] of Object.entries(topics)) {
            // Match subtopics first
            if (data.subtopics) {
              for (
                const [subtopicKey, subKeywords] of Object.entries(
                  data.subtopics,
                )
              ) {
                for (const subKeyword of subKeywords) {
                  if (title.includes(subKeyword.toLowerCase())) {
                    registerContribution(subtopicKey, fullName, party, type);
                    // ⚡ No return, keep matching!
                    break; // You can break inner loop (subKeywords) if matched
                  }
                }
              }
            }

            // Match main topic keywords
            for (const keyword of data.keywords) {
              if (title.includes(keyword.toLowerCase())) {
                registerContribution(topicKey, fullName, party, type);
                // ⚡ No return, keep matching!
                break; // break inner loop (keywords) but continue outer
              }
            }
          }
        });
      };

      const registerContribution = (key, fullName, party, type) => {
        if (!topicMemberCounts.has(key)) {
          topicMemberCounts.set(key, new Map());
        }

        const memberCounts = topicMemberCounts.get(key);

        if (!memberCounts.has(fullName)) {
          memberCounts.set(fullName, { questions: 0, propositions: 0, party });
        }

        memberCounts.get(fullName)[type]++;
      };

      countContribution(member.questions, "questions");
      countContribution(member.propositions, "propositions");
    });

    // After counting, sort and prepare output
    const topContributorsByTopic = {};

    for (const [topicKey, memberCounts] of topicMemberCounts.entries()) {
      const sorted = Array.from(memberCounts.entries())
        .sort((a, b) => {
          const aTotal = a[1].questions + a[1].propositions;
          const bTotal = b[1].questions + b[1].propositions;
          return bTotal - aTotal;
        })
        .slice(0, 5)
        .map(([name, counts]) => ({
          party: counts.party,
          name,
          total: counts.questions + counts.propositions,
          questions: counts.questions,
          propositions: counts.propositions,
        }));

      topContributorsByTopic[topicKey] = sorted;
    }

    members.forEach((member) => {
      // Calculate attendance percentage.
      var elegibleVoteCount = votesRows.filter(
        (row) => new Date(row[3]) >= new Date(member.start_date),
      ).length;
      member.attendance = member.votes.length / elegibleVoteCount; //votesRows.length;

      // ALTERTNATIVE CALCULATION
      // Get unique eligible voting days after member start
      const elegibleDays = new Set(
        votesRows
          .filter((row) => new Date(row[3]) >= new Date(member.start_date))
          .map((row) => row[3]),
      );
      // Get unique days member actually voted
      const attendedDays = new Set(
        member.votes.map((vote) => vote.date),
      );
      // Attendance = proportion of eligible days where they voted at least once
      member.normalizedAttendance = elegibleDays.size === 0
        ? 0
        : attendedDays.size / elegibleDays.size;

      // Calculate outlier percentage.
      const totalVotes = member.votes.length;
      const outlierVotes = member.votes.filter((vote) => vote.outlier).length;
      member.outlier = totalVotes > 0
        ? 100 - Math.round((outlierVotes / totalVotes) * 1000) / 10 // rounded to 1 decimal
        : 100;
    });

    return {
      memberCount,
      members,
      ages,
      incomes2023,
      parties,
      graph: { nodes, links: partyLinks },
      topContributorsByTopic,
    };
  } catch (error) {
    console.error("Error reading Parquet file:", error);
    return { memberCount: 0, members: [], ages: [], incomes2023: [] };
  }
}
