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
let trainees_count = 0;
let users_count = 0;
let entrepreneurs_count = 0;
let traning_title_fd_count = 0;
let attendance_count = 0;
let msme_count = 0;

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
    TABLE_SCHEMA = '${db1Config.database}' 
    AND TABLE_NAME = '${tableName}'
    AND EXTRA LIKE '%auto_increment%';
`);
  return results.map((row) => row.COLUMN_NAME);
};

const copyData = async (db1Connection, table) => {
  const columns = await getColumns(db1Connection, table);

  const autIncCol = await getAutoIncCol(db1Connection, table);

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

  // // Query to copy data from table1 to new_table
  // const copyFromTable1Query = `
  //       INSERT IGNORE INTO \`${table}\` (\`${columnsList}\`)
  //       SELECT ${placeholders}
  //       FROM \`${db1Config.database}\`.\`${table}\`;
  //   `;

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
  await db1Connection.query(disableFKChecks);

  await db1Connection.query(disableStrictMode);

  // await db1Connection.query(dropTableQuery);

  // await db1Connection.query(createTableQuery);

  //   if (autIncCol[0]) await db3Connection.query(disableAutoIncrement);

  // Data Backup

  // if (
  //   ![
  //     "backend_menus",
  //     "menus",
  //     "menu_category",
  //     "menu_locations",
  //     "menu_nodes",
  //     "pages",
  //     "categories",
  //     "posts",
  //     "simple_sliders",
  //     "simple_slider_items",
  //     "tags",
  //     "settings",
  //   ].includes(table)
  // ) {
  await db1Connection.query(`TRUNCATE TABLE \`${table}\``);

  const [results] = await db1Connection.query(copyFromTable2Query);
  console.log(`Table: ${table}, Rows inserted: ${results.affectedRows}`);
  // }
  // else if (
  //   ![
  //     "annual_action_plan",
  //     "trainees",
  //     "training_title",
  //     "training_title_financial_details",
  //   ].includes(table)
  // ) {
  //   const [results] = await db3Connection.query(copyFromTable1Query);
  //   console.log("Table: Stage, Rows inserted:", results.affectedRows);
  // }

  //   if (autIncCol[0]) await db3Connection.query(enableAutoIncrement);

  await db1Connection.query(enableFKChecks);

  await db1Connection.query(enableStrictMode);
};

const syncAnnualActionPlan = async (db1Connection, db2Connection) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db1Connection, "annual_action_plan");

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
        `SELECT id FROM annual_action_plan where financial_year_id = 5`
      );

      for (const row of results) {
        const id = row["id"];
        // Query to copy data from table2 to new_table
        const query = `INSERT INTO annual_action_plan (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.annual_action_plan where id = ${id};`;

        const [results] = await db1Connection.query(query);

        await syncTrainingTitles(
          db1Connection,
          db2Connection,
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
  db1Connection,
  db2Connection,
  financial_year_id,
  old_annual_plan_id,
  new_annual_plan_id
) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db1Connection, "training_title");

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
        `SELECT id FROM training_title where financial_year_id = ${financial_year_id} and annual_action_plan_id = ${old_annual_plan_id}`
      );

      for (const row of results) {
        const id = row["id"];
        // Query to copy data from table2 to new_table
        const query = `INSERT INTO training_title (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.training_title WHERE id = ${id};`;

        const [results] = await db1Connection.query(query);

        await syncTTFD(
          db1Connection,
          db2Connection,
          financial_year_id,
          old_annual_plan_id,
          new_annual_plan_id,
          id,
          results.insertId
        );

        await syncTrainees(
          db1Connection,
          db2Connection,
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
  db1Connection,
  db2Connection,
  financial_year_id,
  old_annual_plan_id,
  new_annual_plan_id,
  old_training_title_id,
  new_training_title_id
) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(
        db1Connection,
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
        `SELECT id FROM training_title_financial_details where financial_year_id = ${financial_year_id} and annual_action_plan_id = ${old_annual_plan_id} and training_title_id = ${old_training_title_id}`
      );

      for (const row of results) {
        const id = row["id"];
        // Query to copy data from table2 to new_table
        const query = `INSERT INTO training_title_financial_details (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.training_title_financial_details WHERE id = ${id};`;

        const [results] = await db1Connection.query(query);

        traning_title_fd_count += results.affectedRows;
      }

      resolve();
    } catch (error) {
      reject(error);
    }
  });
};

const syncTrainees = async (
  db1Connection,
  db2Connection,
  financial_year_id,
  old_annual_plan_id,
  new_annual_plan_id,
  old_training_title_id,
  new_training_title_id
) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db1Connection, "trainees");

      const [results] = await db2Connection.query(
        `SELECT id, entrepreneur_id, user_id FROM trainees where financial_year_id = ${financial_year_id} and annual_action_plan_id = ${old_annual_plan_id} and training_title_id = ${old_training_title_id}`
      );

      for (const row of results) {
        const id = row["id"];
        const old_user_id = row["user_id"];
        const old_entrepreneur_id = row["entrepreneur_id"];

        const new_user_id = await syncUser(
          db1Connection,
          db2Connection,
          old_user_id
        );

        const new_entrepreneur_id = await syncEntrepreneurs(
          db1Connection,
          db2Connection,
          old_entrepreneur_id,
          new_user_id
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
            } else if (col == "entrepreneur_id") {
              return `'${new_entrepreneur_id}'`;
            } else if (col == "user_id") {
              return `'${new_user_id}'`;
            } else {
              return `\`${col}\``;
            }
          })
          .join(",")
          .replace("`id`,", "");

        // Query to copy data from table2 to new_table
        const query = `INSERT INTO trainees (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.trainees WHERE id = ${id};`;

        const [results] = await db1Connection.query(query);

        await syncAttendance(
          db1Connection,
          db2Connection,
          financial_year_id,
          old_annual_plan_id,
          new_annual_plan_id,
          old_training_title_id,
          new_training_title_id,
          old_entrepreneur_id,
          new_entrepreneur_id
        );

        trainees_count += results.affectedRows;
      }

      resolve();
    } catch (error) {
      reject(error);
    }
  });
};

const syncUser = async (db1Connection, db2Connection, old_user_id) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db1Connection, "users");

      // Create the column list and placeholders for the SQL query
      const placeholders = columns
        .map((col) => `\`${col}\``)
        .join(",")
        .replace("`id`,", "");

      const [user_data] = await db2Connection.query(
        `SELECT email FROM users where id = ${old_user_id}`
      );

      if (!user_data[0]?.email) {
        return reject("Base data doesn't exist.");
      }

      const [user_result] = await db1Connection.query(
        `SELECT id FROM users where email = '${user_data[0].email}'`
      );

      if (user_result[0]?.id) {
        return resolve(user_result[0]?.id);
      }

      const query = `INSERT INTO users (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.users WHERE email = '${user_data[0]?.email}'`;

      const [results] = await db1Connection.query(query);

      users_count += results.affectedRows;

      resolve(results.insertId);
    } catch (error) {
      reject(error);
    }
  });
};

const syncEntrepreneurs = async (
  db1Connection,
  db2Connection,
  old_entrepreneur_id,
  new_user_id
) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db1Connection, "entrepreneurs");

      // Create the column list and placeholders for the SQL query
      const placeholders = columns
        .map((col) => {
          if (col == "user_id") {
            return `'${new_user_id}'`;
          } else {
            return `\`${col}\``;
          }
        })
        .join(",")
        .replace("`id`,", "");

      const [ent_data] = await db2Connection.query(
        `SELECT id, email FROM entrepreneurs where id = ${old_entrepreneur_id}`
      );

      if (!ent_data[0]?.email) {
        return reject("Base data doesn't exist.");
      }

      const [ent_result] = await db1Connection.query(
        `SELECT id, user_id FROM entrepreneurs where email = '${ent_data[0]?.email}'`
      );

      await syncMSME(db1Connection, ent_data[0]?.email);

      if (ent_result[0]?.id) {
        if (ent_result[0]?.user_id != new_user_id) {
          const query = `UPDATE entrepreneurs SET user_id = '${new_user_id}' WHERE id = ${old_entrepreneur_id}`;

          await db1Connection.query(query);
        }

        return resolve(old_entrepreneur_id);
      }

      const query = `INSERT INTO entrepreneurs (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.entrepreneurs WHERE email = '${ent_data[0]?.email}'`;

      const [results] = await db1Connection.query(query);

      entrepreneurs_count += results.affectedRows;

      resolve(results.insertId);
    } catch (error) {
      reject(error);
    }
  });
};

const syncMSME = async (db1Connection, email) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db1Connection, "msme_candidate_details");

      // Create the column list and placeholders for the SQL query
      const placeholders = columns
        .map((col) => `\`${col}\``)
        .join(",")
        .replace("`id`,", "");

      const [msme_result] = await db1Connection.query(
        `SELECT id FROM msme_candidate_details where email = '${email}'`
      );

      if (!msme_result[0]?.id) {
        const query = `INSERT INTO msme_candidate_details (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.msme_candidate_details WHERE email = '${email}'`;

        const [results] = await db1Connection.query(query);

        msme_count += results.affectedRows;
      }

      resolve();
    } catch (error) {
      reject(error);
    }
  });
};

const syncAttendance = async (
  db1Connection,
  db2Connection,
  financial_year_id,
  old_annual_plan_id,
  new_annual_plan_id,
  old_training_title_id,
  new_training_title_id,
  old_entrepreneur_id,
  new_entrepreneur_id
) => {
  return new Promise(async (resolve, reject) => {
    try {
      const columns = await getColumns(db1Connection, "attendance");

      // Create the column list and placeholders for the SQL query
      const placeholders = columns
        .map((col) => {
          if (col == "submit_date") {
            return `CASE WHEN submit_date = '0000-00-00' THEN NULL ELSE submit_date END as submit_date`;
          } else if (col == "annual_action_plan_id") {
            return `'${new_annual_plan_id}'`;
          } else if (col == "training_title_id") {
            return `'${new_training_title_id}'`;
          } else if (col == "entrepreneur_id") {
            return `'${new_entrepreneur_id}'`;
          } else {
            return `\`${col}\``;
          }
        })
        .join(",")
        .replace("`id`,", "");

      const [att_result] = await db2Connection.query(
        `SELECT COUNT(id) as count FROM attendance where financial_year_id = ${financial_year_id} and annual_action_plan_id = ${old_annual_plan_id} and training_title_id = ${old_training_title_id} and entrepreneur_id = ${old_entrepreneur_id}`
      );

      if (att_result[0]?.count > 0) {
        // Query to copy data from table2 to new_table
        const query = `INSERT INTO attendance (${columns.join("`,`").replace("id`,", "")}\`) SELECT ${placeholders} FROM \`${db2Config.database}\`.attendance WHERE financial_year_id = ${financial_year_id} and annual_action_plan_id = ${old_annual_plan_id} and training_title_id = ${old_training_title_id} and entrepreneur_id = ${old_entrepreneur_id}`;

        const [results] = await db1Connection.query(query);

        attendance_count += results.affectedRows;
      }

      resolve();
    } catch (error) {
      reject(error);
    }
  });
};

const compareDatabases = async () => {
  const db1Connection = await mysql.createConnection(db1Config);
  const db2Connection = await mysql.createConnection(db2Config);
  // const db3Connection = await mysql.createConnection(db3Config);

  try {
    const [tables] = await Promise.all([
      //   db1Connection.query("SHOW TABLES"),
      db1Connection.query("SHOW TABLES"),
    ]);

    // const tableNames1 = tables1[0].map((row) => Object.values(row)[0]);
    const tableNames = tables[0].map((row) => Object.values(row)[0]);

    // const commonTables = tableNames1.filter((table) =>
    //   tableNames2.includes(table)
    // );

    for (const table of tableNames) {
      if (
        [
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
          "language_meta",
        ].includes(table)
      ) {
        console.log("Processing Table:", table);
        await copyData(db1Connection, table);
      }
    }

    // Remove SQL Strict Mode
    db1Connection.query(`SET SESSION sql_mode = ''`);

    await syncAnnualActionPlan(db1Connection, db2Connection);

    // Remove SQL Strict Mode
    db1Connection.query(
      `SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'`
    );

    console.log("Annual Plan Count :", annual_count);
    console.log("Training Title Count :", traning_title_count);
    console.log("User Count :", users_count);
    console.log("Entrepreneurs Count :", entrepreneurs_count);
    console.log("Trainees Count :", trainees_count);
    console.log("Training Title FD Count :", traning_title_fd_count);
    console.log("Attendance Count :", attendance_count);
    console.log("MSME Count :", msme_count);
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    // await db1Connection.end();
    await db1Connection.end();
    await db2Connection.end();
  }
};

compareDatabases().catch((err) => console.error(err));
