import { DuckDBInstance } from "@duckdb/node-api";
import fs from "fs";

export default async function () {
  try {
    const commissionsFilePath = "src/data/commissions.parquet";
    const membersFilePath = "src/data/members.parquet";

    if (
      !fs.existsSync(commissionsFilePath) || !fs.existsSync(membersFilePath)
    ) {
      console.error("Required Parquet file(s) not found.");
      return { commissions: [], memberParties: {} };
    }

    const instance = await DuckDBInstance.create(":memory:");
    const connection = await instance.connect();

    // Load members to map names -> parties
    const membersResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${membersFilePath}')`,
    );
    const membersRows = membersResult.getRows();

    const memberPartyLookup = {};
    membersRows.forEach((row) => {
      const fullName = `${row[2]} ${row[3]}`.trim(); // first + last name
      memberPartyLookup[fullName.toLowerCase()] = row[9]; // party
    });

    // Load commissions
    const commissionsResult = await connection.runAndReadAll(
      `SELECT * FROM read_parquet('${commissionsFilePath}')`,
    );
    const commissionsRows = commissionsResult.getRows();

    const commissions = commissionsRows.map((row) => {
      const name = row[0];
      const type = row[1];

      // Helper to split members and map to {name, party}
      const mapMembers = (str) =>
        str
          ? str.split(",").map((m) => {
            const name = m.trim();
            return {
              name,
              party: memberPartyLookup[name.toLowerCase()] || "Unknown",
            };
          })
          : [];

      return {
        name,
        type,
        chairs: mapMembers(row[2]),
        subchairs: mapMembers(row[3]),
        permanent_members: mapMembers(row[4]),
        replacement_members: mapMembers(row[5]),
      };
    });

    await connection.close();

    return {
      commissions,
      memberParties: memberPartyLookup,
    };
  } catch (error) {
    console.error("Error reading Parquet file:", error);
    return { commissions: [], memberParties: {} };
  }
}
