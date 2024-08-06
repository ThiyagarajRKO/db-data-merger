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

let annual_count = 0;
let traning_title_count = 0;
let trainees = 0;
let traning_title_fd_count = 0;

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
  //   await db3Connection.query(disableFKChecks);

  await db3Connection.query(disableStrictMode);

  await db3Connection.query(dropTableQuery);

  await db3Connection.query(createTableQuery);

  //   if (autIncCol[0]) await db3Connection.query(disableAutoIncrement);

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

  if (
    ![
      "annual_action_plan",
      "trainees",
      "training_title",
      "training_title_financial_details",
    ].includes(table)
  ) {
    const [result2] = await db3Connection.query(copyFromTable2Query);
    console.log("Table: Stage, Rows inserted:", result2.affectedRows);
  }

  //   if (autIncCol[0]) await db3Connection.query(enableAutoIncrement);

  //   await db3Connection.query(enableFKChecks);

  await db3Connection.query(enableStrictMode);
};

const syncAnnualActionPlan = async (db2Connection, db3Connection) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db2Connection, "annual_action_plan");

      // Create the column list and placeholders for the SQL query
      const placeholders = columns
        .map((col) => {
          if (col == "submit_date") {
            return `CASE WHEN submit_date = '0000-00-00' THEN NULL ELSE submit_date END as submit_date`;
          } else {
            return `\`${col}\``;
          }
        })
        .join(",")
        .replace("`id`,", "");

      const [results] = await db2Connection.query(
        `SELECT id FROM annual_action_plan where financial_year_id = 5 and deleted_at IS NULL`
      );

      for (const row of results) {
        const id = row["id"];
        // Query to copy data from table2 to new_table
        const query = `INSERT INTO annual_action_plan (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.annual_action_plan where id = ${id};`;

        const [results] = await db3Connection.query(query);

        await syncTrainingTitles(
          db2Connection,
          db3Connection,
          5,
          id,
          results.insertId
        );

        annual_count += results.affectedRows;
      }

      resolve();
    } catch (error) {
      reject(error);
    }
  });
};

const syncTrainingTitles = async (
  db2Connection,
  db3Connection,
  financial_year_id,
  old_annual_plan_id,
  new_annual_plan_id
) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db2Connection, "training_title");

      // Create the column list and placeholders for the SQL query
      const placeholders = columns
        .map((col) => {
          if (col == "submit_date") {
            return `CASE WHEN submit_date = '0000-00-00' THEN NULL ELSE submit_date END as submit_date`;
          } else if (col == "annual_action_plan_id") {
            return `'${new_annual_plan_id}'`;
          } else {
            return `\`${col}\``;
          }
        })
        .join(",")
        .replace("`id`,", "");

      const [results] = await db2Connection.query(
        `SELECT * FROM training_title where financial_year_id = ${financial_year_id} and annual_action_plan_id = ${old_annual_plan_id} and deleted_at IS NULL`
      );

      for (const row of results) {
        const id = row["id"];
        // Query to copy data from table2 to new_table
        const query = `INSERT INTO training_title (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.training_title WHERE id = ${id};`;

        const [results] = await db3Connection.query(query);

        await syncTTFD(
          db2Connection,
          db3Connection,
          financial_year_id,
          old_annual_plan_id,
          new_annual_plan_id,
          id,
          results.insertId
        );

        await syncTrainees(
          db2Connection,
          db3Connection,
          financial_year_id,
          old_annual_plan_id,
          new_annual_plan_id,
          id,
          results.insertId
        );

        traning_title_count += results.affectedRows;
      }

      resolve();
    } catch (error) {
      reject(error);
    }
  });
};

const syncTTFD = async (
  db2Connection,
  db3Connection,
  financial_year_id,
  old_annual_plan_id,
  new_annual_plan_id,
  old_training_title_id,
  new_training_title_id
) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(
        db2Connection,
        "training_title_financial_details"
      );

      // Create the column list and placeholders for the SQL query
      const placeholders = columns
        .map((col) => {
          if (col == "submit_date") {
            return `CASE WHEN submit_date = '0000-00-00' THEN NULL ELSE submit_date END as submit_date`;
          } else if (col == "annual_action_plan_id") {
            return `'${new_annual_plan_id}'`;
          } else if (col == "training_title_id") {
            return `'${new_training_title_id}'`;
          } else {
            return `\`${col}\``;
          }
        })
        .join(",")
        .replace("`id`,", "");

      const [results] = await db2Connection.query(
        `SELECT * FROM training_title_financial_details where financial_year_id = ${financial_year_id} and annual_action_plan_id = ${old_annual_plan_id} and training_title_id = ${old_training_title_id} and deleted_at IS NULL`
      );

      for (const row of results) {
        const id = row["id"];
        // Query to copy data from table2 to new_table
        const query = `INSERT INTO training_title_financial_details (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.training_title_financial_details WHERE id = ${id};`;

        const [results] = await db3Connection.query(query);

        traning_title_fd_count += results.affectedRows;
      }

      resolve();
    } catch (error) {
      reject(error);
    }
  });
};

const syncTrainees = async (
  db2Connection,
  db3Connection,
  financial_year_id,
  old_annual_plan_id,
  new_annual_plan_id,
  old_training_title_id,
  new_training_title_id
) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db2Connection, "trainees");

      // Create the column list and placeholders for the SQL query
      const placeholders = columns
        .map((col) => {
          if (col == "submit_date") {
            return `CASE WHEN submit_date = '0000-00-00' THEN NULL ELSE submit_date END as submit_date`;
          } else if (col == "annual_action_plan_id") {
            return `'${new_annual_plan_id}'`;
          } else if (col == "training_title_id") {
            return `'${new_training_title_id}'`;
          } else {
            return `\`${col}\``;
          }
        })
        .join(",")
        .replace("`id`,", "");

      const [results] = await db2Connection.query(
        `SELECT * FROM trainees where financial_year_id = ${financial_year_id} and annual_action_plan_id = ${old_annual_plan_id} and training_title_id = ${old_training_title_id} and deleted_at IS NULL`
      );

      for (const row of results) {
        const id = row["id"];
        // Query to copy data from table2 to new_table
        const query = `INSERT INTO trainees (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.trainees WHERE id = ${id};`;

        const [results] = await db3Connection.query(query);

        trainees += results.affectedRows;
      }

      resolve();
    } catch (error) {
      reject(error);
    }
  });
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

    // Remove SQL Strict Mode
    db3Connection.query(`SET SESSION sql_mode = ''`);

    await syncAnnualActionPlan(db2Connection, db3Connection);

    // Remove SQL Strict Mode
    db2Connection.query(
      `SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'`
    );

    console.log("Annual Plan Count :", annual_count);
    console.log("Training Title Count :", traning_title_count);
    console.log("Trainees Count :", trainees);
    console.log("Training Title FD Count :", traning_title_fd_count);
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    // await db1Connection.end();
    await db2Connection.end();
    await db3Connection.end();
  }
};

compareDatabases().catch((err) => console.error(err));
