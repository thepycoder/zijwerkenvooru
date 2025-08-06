import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';

export default async function () {
  try {
    const commissionsFilePath = 'src/data/commissions.parquet';

    if (!fs.existsSync(commissionsFilePath)) {
      console.error('Parquet file(s) not found.');
      return { commissions: [] };
    }

    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();

    const commissionsResult = await connection.runAndReadAll(`SELECT * FROM read_parquet('${commissionsFilePath}')`);
    const commissionsRows = commissionsResult.getRows();


    let commissions = {}

    commissionsRows.forEach(row => {
      const sessionId = row[0];
      const commissionsId = row[1];

      if (!commissions[sessionId]) {
        commissions[sessionId] = [];
      }

      commissions[sessionId].push({
        session_id: sessionId,
        commission_id: commissionsId,
        date: row[2],
        start_time: row[3],
        end_time: row[4],
        chair: row[6],
      });
    });


    await connection.close();

    const allCommissions = Object.keys(commissions).flatMap(sessionId =>
        commissions[sessionId].map(commission => ({
          ...commission,
          session_id: sessionId,
          commissions_id: commission.commission_id
        }))
    );

    return { commissions: allCommissions };
  } catch (error) {
    console.error('Error reading Parquet file:', error);
    return { commissions: [] };
  }
}
