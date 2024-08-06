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

const db3Config = {
  host: process.env.DB3_HOST,
  user: process.env.DB3_USER,
  password: process.env.DB3_PASSWORD,
  database: process.env.DB3_DATABASE,
};

// Function to get columns dynamically from a table
const getColumns = async (connection, tableName) => {
  const [results] = await connection.query(`SHOW COLUMNS FROM ${tableName}`);
  return results.map((row) => row.Field);
};

const getAutoIncCol = async (connection, tableName) => {
  const [results] = await connection.query(`SELECT 
    COLUMN_NAME 
FROM 
    information_schema.COLUMNS 
WHERE 
    TABLE_SCHEMA = '${db3Config.database}' 
    AND TABLE_NAME = '${tableName}'
    AND EXTRA LIKE '%auto_increment%';
`);
  return results.map((row) => row.COLUMN_NAME);
};

const copyData = async (db2Connection, db3Connection, table) => {
  const columns = await getColumns(db2Connection, table);

  const autIncCol = await getAutoIncCol(db2Connection, table);

  // Create the column list and placeholders for the SQL query
  let columnsList = columns.join("`,`");
  let placeholders = columns
    .map((col) => {
      if (col == "submit_date") {
        return `CASE WHEN submit_date = '0000-00-00' THEN NULL ELSE submit_date END as submit_date`;
      } else {
        return `\`${col}\``;
      }
    })
    .join(",");

  // Disable foreign key checks (if necessary)
  const disableFKChecks = `SET FOREIGN_KEY_CHECKS=0`;

  const disableStrictMode = `SET SESSION sql_mode = ''`;

  // Query to create the new table (if not exists)
  const dropTableQuery = `
        DROP TABLE IF EXISTS \`${table}\``;

  // Query to create the new table (if not exists)
  const createTableQuery = `
        CREATE TABLE \`${table}\` LIKE \`${db1Config.database}\`.\`${table}\`;
    `;

  // Disable auto increment temporarily
  const disableAutoIncrement = `
        ALTER TABLE \`${table}\` MODIFY COLUMN ${autIncCol[0]} INT NOT NULL;
    `;

  // Query to copy data from table1 to new_table
  const copyFromTable1Query = `
        INSERT IGNORE INTO \`${table}\` (\`${columnsList}\`)
        SELECT ${placeholders}
        FROM \`${db1Config.database}\`.\`${table}\`;
    `;

  // Query to copy data from table2 to new_table
  const copyFromTable2Query = `
        INSERT IGNORE INTO \`${table}\` (\`${columnsList}\`)
        SELECT ${placeholders}
        FROM \`${db2Config.database}\`.\`${table}\`;
    `;

  // Re-enable auto increment on the id column
  const enableAutoIncrement = `
        ALTER TABLE \`${table}\` MODIFY COLUMN ${autIncCol[0]} INT NOT NULL AUTO_INCREMENT;
    `;

  // Re-enable foreign key checks
  const enableFKChecks = `SET FOREIGN_KEY_CHECKS=1`;

  const enableStrictMode = `SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'`;

  // Execute the queries sequentially
  await db3Connection.query(disableFKChecks);

  await db3Connection.query(disableStrictMode);

  await db3Connection.query(dropTableQuery);

  await db3Connection.query(createTableQuery);

  if (autIncCol[0]) await db3Connection.query(disableAutoIncrement);

  // Data Backup

  //   "attribute_options",
  //       "categories",
  //       "dashboard_widget_settings",
  //       "media_files",
  //       "media_folders",
  //       "media_settings",
  //       "menus",
  //       "menu_locations",
  //       "menu_nodes",
  //       "meta_boxes",
  //       "pages",
  //       "settings",
  //       "simple_sliders",
  //       "simple_slider_items",

  const [result2] = await db3Connection.query(copyFromTable2Query);
  console.log("Table: Stage, Rows inserted:", result2.affectedRows);

  if (
    ![
      "backend_menus",
      "menus",
      "menu_category",
      "menu_locations",
      "menu_nodes",
      "pages",
      "categories",
      "posts",
      "simple_sliders",
      "simple_slider_items",
      "tags",
      "settings",
    ].includes(table)
  ) {
    const [result1] = await db3Connection.query(copyFromTable1Query);
    console.log("Table: Live, Rows inserted:", result1.affectedRows);
  }

  if (autIncCol[0]) await db3Connection.query(enableAutoIncrement);

  await db3Connection.query(enableFKChecks);

  await db3Connection.query(enableStrictMode);
};

const compareDatabases = async () => {
  //   const db1Connection = await mysql.createConnection(db1Config);
  const db2Connection = await mysql.createConnection(db2Config);
  const db3Connection = await mysql.createConnection(db3Config);

  try {
    const [tables] = await Promise.all([
      //   db1Connection.query("SHOW TABLES"),
      db2Connection.query("SHOW TABLES"),
    ]);

    // const tableNames1 = tables1[0].map((row) => Object.values(row)[0]);
    const tableNames = tables[0].map((row) => Object.values(row)[0]);

    // const commonTables = tableNames1.filter((table) =>
    //   tableNames2.includes(table)
    // );

    for (const table of tableNames) {
      console.log("Processing Table:", table);
      await copyData(db2Connection, db3Connection, table);
    }
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    // await db1Connection.end();
    await db2Connection.end();
    await db3Connection.end();
  }
};

compareDatabases().catch((err) => console.error(err));
