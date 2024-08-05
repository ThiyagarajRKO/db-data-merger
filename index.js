require("dotenv").config();
const mysql = require("mysql2/promise");

const db1Config = {
  host: process.env.DB1_HOST,
  user: process.env.DB1_USER,
  password: process.env.DB1_PASSWORD,
  database: process.env.DB1_DATABASE,
};

const db2Config = {
  host: process.env.DB2_HOST,
  user: process.env.DB2_USER,
  password: process.env.DB2_PASSWORD,
  database: process.env.DB2_DATABASE,
};

// Function to get columns dynamically from a table
const getColumns = async (connection, tableName) => {
  const [results] = await connection.query(`SHOW COLUMNS FROM ${tableName}`);
  return results.map((row) => row.Field);
};

const getPrimaryKey = async (connection, tableName) => {
  const [results] = await connection.query(`SELECT COLUMN_NAME
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = '${db2Config.database}'
  AND TABLE_NAME = '${tableName}'
  AND COLUMN_KEY = 'PRI'`);
  return results.map((row) => row.COLUMN_NAME);
};

const syncTables = async (db1Connection, db2Connection, table) => {
  try {
    const columnsA = await getColumns(db1Connection, table);
    const columnsB = await getColumns(db2Connection, table);

    const primaryKey = await getPrimaryKey(db2Connection, table);

    // Check if both tables have the same columns
    if (!columnsB.every((col) => columnsA.includes(col))) {
      throw new Error("Columns do not match");
    }

    let primary_key = primaryKey[0];

    if (!primary_key)
      switch (table) {
        case "password_resets":
          primary_key = "email";
          break;
        default:
          primary_key = "id";
      }

    // Create the column list and placeholders for the SQL query
    let columnsList = columnsB.join("`,`");
    let placeholders = columnsB.map((col) => `b.${col}`).join(", ");

    // Prepare the SQL query
    let insertQuery = `INSERT INTO \`${db1Config.database}\`.\`${table}\` (\`${columnsList}\`)
                         SELECT ${placeholders}
                         FROM \`${db2Config.database}\`.\`${table}\` b
                         LEFT JOIN \`${db1Config.database}\`.\`${table}\` a ON b.${primary_key} = a.${primary_key}
                         WHERE a.${primary_key} IS NULL`;

    // Execute the query
    const [results] = await db2Connection.query(insertQuery);
    console.log("Rows inserted:", results.affectedRows);
  } catch (err) {
    console.error("Error:", err.message);
  }
};

const compareDatabases = async () => {
  const db1Connection = await mysql.createConnection(db1Config);
  const db2Connection = await mysql.createConnection(db2Config);

  try {
    const [tables1, tables2] = await Promise.all([
      db1Connection.query("SHOW TABLES"),
      db2Connection.query("SHOW TABLES"),
    ]);

    const tableNames1 = tables1[0].map((row) => Object.values(row)[0]);
    const tableNames2 = tables2[0].map((row) => Object.values(row)[0]);

    const commonTables = tableNames1.filter((table) =>
      tableNames2.includes(table)
    );

    for (const table of commonTables) {
      console.log("Processing Table:", table);
      await syncTables(db1Connection, db2Connection, table);
    }
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    await db1Connection.end();
    await db2Connection.end();
  }
};

compareDatabases().catch((err) => console.error(err));
