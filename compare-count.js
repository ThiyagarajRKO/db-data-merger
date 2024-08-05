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

async function compareColumns(table) {
  const [columns1, columns2] = await Promise.all([
    db1Connection.query(`SHOW COLUMNS FROM ${table}`),
    db2Connection.query(`SHOW COLUMNS FROM ${table}`),
  ]);

  const columnMap1 = new Map(columns1[0].map((col) => [col.Field, col]));
  const columnMap2 = new Map(columns2[0].map((col) => [col.Field, col]));

  let columnDifferenceCount = 0;

  columnMap1.forEach((value, key) => {
    if (
      !columnMap2.has(key) ||
      JSON.stringify(value) !== JSON.stringify(columnMap2.get(key))
    ) {
      columnDifferenceCount++;
    }
  });

  columnMap2.forEach((value, key) => {
    if (!columnMap1.has(key)) {
      columnDifferenceCount++;
    }
  });

  return columnDifferenceCount;
}

async function compareTables(table) {
  const [rows1, rows2] = await Promise.all([
    db1Connection.query(`DESCRIBE ${table}`),
    db2Connection.query(`DESCRIBE ${table}`),
  ]);

  const schema1 = rows1[0];
  const schema2 = rows2[0];

  if (JSON.stringify(schema1) !== JSON.stringify(schema2)) {
    return 1; // Return 1 for each schema difference found
  }
  return 0;
}

async function compareDataByPrimaryKey(table, primaryKey) {
  const [rows1, rows2] = await Promise.all([
    db1Connection.query(`SELECT * FROM ${table}`),
    db2Connection.query(`SELECT * FROM ${table}`),
  ]);

  const data1 = rows1[0];
  const data2 = rows2[0];

  const map1 = new Map(data1.map((row) => [row[primaryKey], row]));
  const map2 = new Map(data2.map((row) => [row[primaryKey], row]));

  let differenceCount = 0;

  map1.forEach((value, key) => {
    if (
      !map2.has(key) ||
      JSON.stringify(value) !== JSON.stringify(map2.get(key))
    ) {
      differenceCount++;
    }
  });

  map2.forEach((value, key) => {
    if (!map1.has(key)) {
      differenceCount++;
    }
  });

  return differenceCount; // Return the count of data differences found
}

async function compareDatabases() {
  db1Connection = await mysql.createConnection(db1Config);
  db2Connection = await mysql.createConnection(db2Config);

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
    const tableDiff = await compareTables(table);
    const colDiff = await compareColumns(table);
    const dataDiff = await compareDataByPrimaryKey(table, "id");

    if (tableDiff > 0 || colDiff > 0 || dataDiff > 0)
      console.log("Table Name : ", table);

    if (tableDiff > 0) console.log("Table", tableDiff);

    if (colDiff > 0) console.log("Column", ColDiff);

    if (dataDiff > 0) console.log("Data", dataDiff);
    // totalSchemaDifferences += await compareTables(table);
    // totalDataDifferences += await compareDataByPrimaryKey(table, "id"); // Replace 'id' with the primary key column name
  }

  db1Connection.end();
  db2Connection.end();
}

compareDatabases().catch((err) => console.error(err));
