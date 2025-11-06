import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';
import crypto from 'crypto';

const meetingsFilePath = 'src/data/meetings.parquet';
const votesFilePath = 'src/data/votes.parquet';
const questionsFilePath = 'src/data/questions.parquet';
const propositionsFilePath = 'src/data/propositions.parquet';
const membersFilePath = 'src/data/members.parquet';
const summariesFilePath = 'src/data/summaries.parquet';
const dossiersFilePath = 'src/data/dossiers.parquet';

export default async function () {
  try {
    if (
      !fs.existsSync(membersFilePath) ||
      !fs.existsSync(questionsFilePath) ||
      !fs.existsSync(propositionsFilePath) ||
      !fs.existsSync(dossiersFilePath) ||
      !fs.existsSync(meetingsFilePath)
    ) {
      console.error('Required Parquet file(s) missing.');
      return {};
    }

    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();

    const [membersRows, questionsRows, propositionsRows, dossiersRows, meetingsRows] = await Promise.all([
      connection.runAndReadAll(`SELECT * FROM read_parquet('${membersFilePath}')`).then(r => r.getRows()),
      connection.runAndReadAll(`SELECT * FROM read_parquet('${questionsFilePath}')`).then(r => r.getRows()),
      connection.runAndReadAll(`SELECT * FROM read_parquet('${propositionsFilePath}')`).then(r => r.getRows()),
      connection.runAndReadAll(`SELECT * FROM read_parquet('${dossiersFilePath}')`).then(r => r.getRows()),
      connection.runAndReadAll(`SELECT * FROM read_parquet('${meetingsFilePath}')`).then(r => r.getRows()),
    ]);

    const parties = {};
    const memberPartyMap = {};
    const memberIdMap = {};

     const meetingDateMap = new Map();
      meetingsRows.forEach(row => {
          const sessionId = row[0];
          const meetingId = row[1];
          const date = row[2];
          const key = `${sessionId}-${meetingId}`;
          meetingDateMap.set(key, date);
      });

    // Build member maps
    membersRows.forEach(row => {
      const memberId = row[0];
      const firstName = row[2];
      const lastName = row[3];

      const name = `${firstName} ${lastName}`.toLowerCase();
      const key = name.trim().toLowerCase().replace(/\s+/g, '-');
      const party = row[9];
      const active = row[12];
      const date_of_birth = row[5];
      const place_of_birth = row[6];
      const language = row[7];
      const constituency = row[8];

      memberIdMap[key] = memberId;

      if (!parties[party]) {
        parties[party] = {
          name: party,
          members: new Set(),
          propositions: [],
          questions: []
        };
      }

      parties[party].members.add(
        { 
          first_name: firstName, 
          last_name: lastName, 
          active: active, 
          date_of_birth: date_of_birth, 
          place_of_birth: place_of_birth, 
          language: language, 
          constituency: constituency
        });
      memberPartyMap[key] = party;
  
    });

    // Add questions to parties
    questionsRows.forEach(q => {
      const questionId = q[0];
      const sessionId = q[1];
      const meetingId = q[2];
      const rawQuestioners = q[3]?.split(',') || [];
      const rawRespondents = q[4]?.split(',') || [];
      const topicsNl = q[5]?.split(';').map(t => t.trim()) || [];
      const topicsFr = q[6]?.split(';').map(t => t.trim()) || [];
      const rawTopicsNl = q[5];
      const discussion = JSON.parse(q[7] || '[]').map(discussionItem => ({
        speaker: discussionItem.speaker,
        text: discussionItem.text
      }));
      const discussionIds = q[8]?.split(',').map(d => d.trim()) || [];

      const keyForDate = `${sessionId}-${meetingId}`;
      const date = meetingDateMap.get(keyForDate) || null;

      rawQuestioners.forEach((name, index) => {
        const trimmed = name.trim().toLowerCase().replace(/\s+/g, '-');
        const party = memberPartyMap[trimmed];
      
        if (party && parties[party]) {
          // Check if the current index's party matches
          const questionDetails = {
            question_id: questionId,
            session_id: sessionId,
            meeting_id: meetingId,
            type: 'plenary', // FIXME: always plenary? no?
            questioners: rawQuestioners.map((name, idx) => ({
              name: name.trim(),
              party: memberPartyMap[name.trim().toLowerCase().replace(/\s+/g, '-')] || "Unknown"
            })),
            respondents: rawRespondents.map(name => ({
              name: name.trim(),
              party: memberPartyMap[name.trim().toLowerCase().replace(/\s+/g, '-')] || "Unknown"
            })),
            topics_nl: [topicsNl[index]].filter(Boolean),  // Only include matching topic
            topics_fr: [topicsFr[index]].filter(Boolean),
            discussion: discussion,
            discussion_ids: discussionIds,
            date: date
          };
      
          parties[party].questions.push(questionDetails);
        }
      });
      
    });

    // Convert Sets to arrays for JSON serialization
    Object.values(parties).forEach(party => {
      party.members = Array.from(party.members);
    });

    return { parties: parties };
  } catch (error) {
    console.error('Error reading Parquet file:', error);
    return {};
  }
}
