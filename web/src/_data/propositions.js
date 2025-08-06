import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';
import crypto from 'crypto';
export default async function () {
    try {
        const propositionsFilePath = 'src/data/propositions.parquet';
        const meetingsFilePath = 'src/data/meetings.parquet';
        const dossiersFilePath = 'src/data/dossiers.parquet';
        const membersFilePath = 'src/data/members.parquet';
        const summariesFilePath = 'src/data/summaries.parquet';

        const allFilesExist = [
            propositionsFilePath,
            meetingsFilePath,
            dossiersFilePath,
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

        const [
            propositionsRows,
            meetingsRows,
            dossiersRows,
            membersRows,
            summariesRows,
        ] = await Promise.all([
            readParquet(propositionsFilePath),
            readParquet(meetingsFilePath),
            readParquet(dossiersFilePath),
            readParquet(membersFilePath),
            readParquet(summariesFilePath),
        ]);

        // Build summary lookup.
        const summaryByHash = {};
        summariesRows.forEach(row => {
            summaryByHash[row[0]] = row[2]; // Assuming row[0] is input_hash and row[2] is summary
        });


        const memberPartyLookup = {};
        membersRows.forEach(row => {
          const memberId = row[2] + " " + row[3];  // Assuming member_id is at index 0
          const party = row[9];     // Assuming party name is at index 1
          memberPartyLookup[memberId] = party;
        });
        
        
        // Utility functions
        const convertDate = (rawDate) => {
            if (!rawDate || typeof rawDate !== 'string') return null;
            const [day, month, year] = rawDate.split('/');
            if (!day || !month || !year) return null;
            return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
        };

        // Lookup dossier by id.
        const dossierById = {};
        dossiersRows.forEach(row => {

            const authors = row[3].split(",").map(q => {
                const name = q.trim();
                return {
                    name: name,
                    party: memberPartyLookup[name] || "Unknown" // Same logic as in votes
                };
            });
            
            const id = row[1];   
            const document_type = row[7];
            const vote_date = convertDate(row[6]);
            const status = row[8];

            dossierById[id] = {
                authors,
                document_type,
                status,
                vote_date
            };
        });

        // Get date from meeting.
        const meetingDateMap = new Map();
        meetingsRows.forEach(row => {
            const sessionId = row[0];
            const meetingId = row[1];
            const date = row[2];
            const key = `${sessionId}-${meetingId}`;
            meetingDateMap.set(key, date);
        });
     
        let propositions = [];

        propositionsRows.forEach(row => {
            const sessionId = row[1];
            const meetingId = row[2];
            const dossier_id = row[5];
            
            const keyForDate = `${sessionId}-${meetingId}`;
            const date = meetingDateMap.get(keyForDate) || null;

            const dossierData = dossierById[dossier_id] || {};

            const rawTitleNl = row[3];
            const hash = hashText(rawTitleNl + ".");
            const title_summary_nl = summaryByHash[hash] || null;

            const proposition = {
                proposition_id: row[0],
                session_id: row[1],
                meeting_id: row[2],
                date: date,
                title_nl: row[3],
                title_fr: row[4],
                title_summary_nl: title_summary_nl,
                dossier_id: row[5],
                authors: dossierData.authors || [],
                document_type: dossierData.document_type || null,
                status: dossierData.status || null,
                vote_date: dossierData.vote_date,
            };

            propositions.push(proposition);
        });

        await connection.close();

        return { propositions: propositions };
    } catch (error) {
        console.error('Error reading Parquet file:', error);
        return { propositions: [] };
    }
}

const hashText = (text) => {
    return crypto.createHash('sha256').update(text).digest('hex');
};