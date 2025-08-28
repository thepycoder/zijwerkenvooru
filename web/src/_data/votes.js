import { DuckDBInstance } from "@duckdb/node-api";
import fs from "fs";
import crypto from "crypto";
export default async function () {
  try {
    const votesFilePath = "src/data/votes.parquet";

    const allFilesExist = [
      votesFilePath,
    ].every(fs.existsSync);

    if (!allFilesExist) {
      console.error("Parquet file(s) not found.");
      return { votes: [] };
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
      votesRows,
    ] = await Promise.all([
      readParquet(votesFilePath),
    ]);

    const votes = [];

    votesRows.forEach((row) => {
      votes.push(row[0]);
    });

    await connection.close();

    return { votes: votes };
  } catch (error) {
    console.error("Error reading Parquet file:", error);
    return { votes: [] };
  }
}
