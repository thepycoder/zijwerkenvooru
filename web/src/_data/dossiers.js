import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';

export default async function () {
    try {
        const dossiersFilePath = 'src/data/dossiers.parquet';
        const subdocumentsFilePath = 'src/data/subdocuments.parquet';
        const membersFilePath = 'src/data/members.parquet';
        const votesFilePath = 'src/data/votes.parquet';

        const allFilesExist = [
            dossiersFilePath,
            subdocumentsFilePath,
            membersFilePath,
            votesFilePath
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
            dossiersRows,
            subdocumentsRows,
            membersRows,
            votesRows
        ] = await Promise.all([
            readParquet(dossiersFilePath),
            readParquet(subdocumentsFilePath),
            readParquet(membersFilePath),
            readParquet(votesFilePath)
        ]);

        // Utility functions.
        const convertDate = (rawDate) => {
            if (!rawDate || typeof rawDate !== 'string') return null;
            const [day, month, year] = rawDate.split('/');
            if (!day || !month || !year) return null;
            return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
        };

        // Lookup party by member.
        const memberPartyLookup = {};
        membersRows.forEach(row => {
            const memberId = row[2] + " " + row[3];  // Assuming member_id is at index 0
            const party = row[9];     // Assuming party name is at index 1
            memberPartyLookup[memberId] = party;
        });

        // Lookup votes by dossier + subdocument.
        const votesByDossierAndDoc = {};
        votesRows.forEach(row => {
            const dossierId = row[12];
            const documentIdRaw = String(row[13]);
            const docIdMatch = documentIdRaw.match(/(\d+)$/); // Extract final number
            const docId = docIdMatch ? docIdMatch[1].padStart(3, '0') : null;

            if (!docId) return;

            const vote = {
                vote_id: row[0],
                session_id: row[1],
                meeting_id: row[2],
                yes_count: row[6],
                no_count: row[7],
                abstain_count: row[8],
                dossier_id: row[12],
                document_id: row[13]
            }

            const key = `${dossierId}_${docId}`;

            if (!votesByDossierAndDoc[key]) {
                votesByDossierAndDoc[key] = [];
            }

            votesByDossierAndDoc[key].push(vote);
        });

        // Map of dossier_id to its subdocuments
        const subdocumentsByDossier = {};
        subdocumentsRows.forEach(row => {
            const dossierId = row[0];

            const subdocId = row[1];
            const voteKey = `${dossierId}_${subdocId}`;

            const subdoc = {
                id: row[1],
                date: convertDate(row[2]),
                type: row[3],
                authors: row[4]
                    ? row[4].split(',').map(author => {
                        const name = author.trim();
                        return {
                            name,
                            party: (memberPartyLookup[name] || "Unknown").trim()
                        };
                    })
                    : [],
                votes: votesByDossierAndDoc[voteKey] || []
            };

            if (!subdocumentsByDossier[dossierId]) {
                subdocumentsByDossier[dossierId] = [];
            }

            subdocumentsByDossier[dossierId].push(subdoc);
        });


        // Build dossiers grouped by session
        let dossiersBySession = {};

        dossiersRows.forEach(row => {
            const sessionId = row[0];
            const dossierId = row[1];
            const dossier = {
                dossier_id: dossierId,
                title: row[2],
                authors: row[3]
                    ? row[3].split(',').map(author => {
                        const name = author.trim();
                        return {
                            name,
                            party: (memberPartyLookup[name] || "Unknown").trim()
                        };
                    })
                    : [],
                submission_date: convertDate(row[4]),
                end_date: convertDate(row[5]),
                vote_date: convertDate(row[6]),
                document_type: row[7],
                status: row[8],
                subdocuments: subdocumentsByDossier[dossierId] || []
            };

            if (!dossiersBySession[sessionId]) {
                dossiersBySession[sessionId] = [];
            }

            dossiersBySession[sessionId].push(dossier);
        });

        // Flatten to a list, adding session_id to each dossier
        const allDossiers = Object.keys(dossiersBySession).flatMap(sessionId =>
            dossiersBySession[sessionId].map(dossier => ({
                ...dossier,
                session_id: sessionId
            }))
        );

        await connection.close();

        return { dossiers: allDossiers };
    } catch (error) {
        console.error('Error reading Parquet file:', error);
        return { dossiers: [] };
    }
}