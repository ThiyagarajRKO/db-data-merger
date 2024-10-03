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

const syncMissingMSMEData = (db1Connection, db2Connection) => {
  return new Promise(async (resolve, reject) => {
    try {
      // // const columnsA = await getColumns(db1Connection, table);
      // const columnsB = await getColumns(db2Connection, table);

      // const primaryKey = await getPrimaryKey(db2Connection, table);

      // SQL count query
      const msmeCountQuery = `SELECT COUNT(id) as count FROM msme_candidate_details WHERE created_at BETWEEN '2024-08-06 00:00:00' AND '2024-09-26 23:59:59'`;

      // Execute the query
      const [msmeResult] = await db2Connection.query(msmeCountQuery);

      const msmeMissingRowCount = msmeResult[0]["count"];

      const limit = 10;
      const totalLoop = Math.ceil(msmeMissingRowCount / limit); // Calculate total number of iterations

      console.log("Total Count : ", msmeMissingRowCount);
      console.log("Total Loop : ", totalLoop);

      for (let i = 0; i < totalLoop; i++) {
        // Prepare the SQL query
        const offset = i * limit; //calculaye this

        const msmeDataQuery = `SELECT * FROM msme_candidate_details WHERE created_at BETWEEN '2024-08-06 00:00:00' AND '2024-09-26 23:59:59' LIMIT ${limit} OFFSET ${offset}`;

        // Execute the query
        const [msmeResult] = await db2Connection.query(msmeDataQuery);

        for (let m = 0; m < msmeResult.length; m++) {
          const currentData = msmeResult[m];

          const newMSMEId = await syncMSME(db1Connection, currentData);

          if (!currentData?.id) continue; // skip if id is null

          const newUserId = await syncUser(
            db1Connection,
            db2Connection,
            currentData?.email
          );

          if (!newUserId) return;

          const entIds = await syncEntrepreneur(
            db1Connection,
            db2Connection,
            newUserId,
            currentData?.id,
            newMSMEId
          );

          await syncTrainee(
            db1Connection,
            db2Connection,
            newUserId,
            entIds?.oldId,
            entIds?.newId
          );
        }
      }

      resolve();
    } catch (err) {
      console.error("Error:", err.message || err);
      reject(err);
    }
  });
};

const syncMSME = (db1Connection, msmeCandidateData) => {
  return new Promise(async (resolve, reject) => {
    try {
      if (!msmeCandidateData) return reject({ message: "Invalid data!" });

      // Getting Entrepreneurs data
      // Inserting MSME Candidate data into another database (db1Connection)
      const msmeCandidateInsertDataQuery = `
    INSERT IGNORE INTO msme_candidate_details (
      msme_type, candidate_msme_ref_id, scheme, candidate_name, care_of, father_husband_name, spouse_name, gender, category, mobile_no, 
      email, dob, qualification, district_id, address, photo, enroll_start_date, enroll_to_date, is_enrolled, created_at, updated_at, 
      deleted_at, old_data_ref_id, entrepreneur_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

      // Prepare the values to insert (in the same order as the columns in your insert query)
      const values = [
        msmeCandidateData.msme_type,
        msmeCandidateData.candidate_msme_ref_id,
        msmeCandidateData.scheme,
        msmeCandidateData.candidate_name,
        msmeCandidateData.care_of,
        msmeCandidateData.father_husband_name,
        msmeCandidateData.spouse_name,
        msmeCandidateData.gender,
        msmeCandidateData.category,
        msmeCandidateData.mobile_no,
        msmeCandidateData.email,
        msmeCandidateData.dob,
        msmeCandidateData.qualification,
        msmeCandidateData.district_id,
        msmeCandidateData.address,
        msmeCandidateData.photo,
        msmeCandidateData.enroll_start_date,
        msmeCandidateData.enroll_to_date,
        msmeCandidateData.is_enrolled,
        msmeCandidateData.created_at,
        msmeCandidateData.updated_at,
        msmeCandidateData.deleted_at,
        msmeCandidateData.old_data_ref_id,
        msmeCandidateData.entrepreneur_id,
      ];

      // Insert data using parameterized query
      const [msmeCandidateInsertResult] = await db1Connection.query(
        msmeCandidateInsertDataQuery,
        values
      );

      // Log the inserted ID
      console.log(
        "MSME Candidate inserted with ID: ",
        msmeCandidateInsertResult?.insertId
      );

      resolve(msmeCandidateInsertResult?.insertId);
    } catch (err) {
      reject(err?.message || err);
    }
  });
};

const syncUser = (db1Connection, db2Connection, email) => {
  return new Promise(async (resolve, reject) => {
    try {
      if (!email) {
        return reject({ message: "Invlaid email!" });
      }

      // Getting User data
      const userDataQuery = `SELECT email, email_verified_at, password, remember_token, created_at, updated_at, first_name, last_name, username, avatar_id, super_user, manage_supers, permissions, last_login, deleted_at, old_data_ref_id, old_data_entrepreneur_ref_id FROM users WHERE email = '${email}' LIMIT 1`;
      const [userResult] = await db2Connection.query(userDataQuery);

      if (userResult.length <= 0) return;

      const userData = userResult[0]; // Extract the first (and only) result

      // Inserting User data into another database (db1Connection)
      const userInsertDataQuery = `
    INSERT IGNORE INTO users (
      email, email_verified_at, password, remember_token, created_at, updated_at, first_name, last_name, username, avatar_id, super_user, manage_supers, permissions, last_login, deleted_at, old_data_ref_id, old_data_entrepreneur_ref_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

      // Prepare the values to insert (in the same order as the columns in your insert query)
      const values = [
        userData.email,
        userData.email_verified_at,
        userData.password,
        userData.remember_token,
        userData.created_at,
        userData.updated_at,
        userData.first_name,
        userData.last_name,
        userData.username,
        userData.avatar_id,
        userData.super_user,
        userData.manage_supers,
        userData.permissions,
        userData.last_login,
        userData.deleted_at,
        userData.old_data_ref_id,
        userData.old_data_entrepreneur_ref_id,
      ];

      // Insert data using parameterized query
      const [userInsertResult] = await db1Connection.query(
        userInsertDataQuery,
        values
      );

      // Log the inserted ID
      console.log("User inserted with ID: ", userInsertResult?.insertId);

      resolve(userInsertResult?.insertId);
    } catch (err) {
      reject(err?.message || err);
    }
  });
};

const syncEntrepreneur = (
  db1Connection,
  db2Connection,
  newUserId,
  oldMSMEId,
  newMSMEId
) => {
  return new Promise(async (resolve, reject) => {
    try {
      if (!newUserId) return reject({ message: "Invalid User Id" });

      if (!newMSMEId || !oldMSMEId)
        return reject({ message: "Invalid MSME Id" });

      // Getting Entrepreneurs data
      const [entResult] = await db2Connection.query(
        `SELECT * FROM entrepreneurs WHERE msme_candidate_detail_id = ${oldMSMEId} LIMIT 1`
      );

      if (entResult.length <= 0) return;

      const entrepreneurData = entResult[0];

      // Inserting Entrepreneur data into another database (db1Connection)
      const entrepreneurInsertDataQuery = `
        INSERT IGNORE INTO entrepreneurs (
          user_id, prefix_id, care_of, community, name, dob, gender_id, mobile, email, password, father_name, aadhaar_no, category_id, 
          state_id, district_id, pincode, address, university_type_id, type_of_college_id, college_name, website_link, photo_path, 
          candidate_type_id, entrepreneurial_category_id, hub_institution_id, spoke_registration_id, student_college_name, student_course_name, 
          student_year, student_school_name, student_standard_name, physically_challenged, activity_name, qualification_id, religion_id, 
          student_type_id, scheme, msme_candidate_detail_id, is_active, note, created_at, updated_at, deleted_at, usertype, old_data_ref_id, 
          district_name, candidate_type_name, religion_name, student_type_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;

      // Prepare the values to insert (in the same order as the columns in your insert query)
      const values = [
        newUserId,
        entrepreneurData.prefix_id,
        entrepreneurData.care_of,
        entrepreneurData.community,
        entrepreneurData.name,
        entrepreneurData.dob,
        entrepreneurData.gender_id,
        entrepreneurData.mobile,
        entrepreneurData.email,
        entrepreneurData.password,
        entrepreneurData.father_name,
        entrepreneurData.aadhaar_no,
        entrepreneurData.category_id,
        entrepreneurData.state_id,
        entrepreneurData.district_id,
        entrepreneurData.pincode,
        entrepreneurData.address,
        entrepreneurData.university_type_id,
        entrepreneurData.type_of_college_id,
        entrepreneurData.college_name,
        entrepreneurData.website_link,
        entrepreneurData.photo_path,
        entrepreneurData.candidate_type_id,
        entrepreneurData.entrepreneurial_category_id,
        entrepreneurData.hub_institution_id,
        entrepreneurData.spoke_registration_id,
        entrepreneurData.student_college_name,
        entrepreneurData.student_course_name,
        entrepreneurData.student_year,
        entrepreneurData.student_school_name,
        entrepreneurData.student_standard_name,
        entrepreneurData.physically_challenged,
        entrepreneurData.activity_name,
        entrepreneurData.qualification_id,
        entrepreneurData.religion_id,
        entrepreneurData.student_type_id,
        entrepreneurData.scheme,
        newMSMEId,
        entrepreneurData.is_active,
        entrepreneurData.note,
        entrepreneurData.created_at,
        entrepreneurData.updated_at,
        entrepreneurData.deleted_at,
        entrepreneurData.usertype,
        entrepreneurData.old_data_ref_id,
        entrepreneurData.district_name,
        entrepreneurData.candidate_type_name,
        entrepreneurData.religion_name,
        entrepreneurData.student_type_name,
      ];

      // Insert data using parameterized query
      const [entrepreneurInsertResult] = await db1Connection.query(
        entrepreneurInsertDataQuery,
        values
      );

      // Log the inserted ID
      console.log(
        "Entrepreneur inserted with ID: ",
        entrepreneurInsertResult?.insertId
      );

      resolve({
        oldId: entrepreneurData?.id,
        newId: entrepreneurInsertResult?.insertId,
      });
    } catch (err) {
      reject(err?.message || err);
    }
  });
};

const syncTrainee = (
  db1Connection,
  db2Connection,
  newUserId,
  oldEntId,
  newEntUId
) => {
  return new Promise(async (resolve, reject) => {
    try {
      if (!oldEntId || !newEntUId)
        return reject({ message: "Invalid Ent Ids" });

      if (!newUserId) return reject({ message: "Invalid User Id" });

      // Getting Trainees Data
      const traineeDataQuery = `SELECT * FROM trainees WHERE entrepreneur_id = ${oldEntId}`;
      const [traineeResult] = await db2Connection.query(traineeDataQuery);
      // console.log("traineeResult :", traineeResult.length);

      if (traineeResult.length <= 0) return;
      const traineeData = traineeResult[0]; // Extract the first (and only) result

      // Inserting Trainee data into another database (db1Connection)
      const traineeInsertDataQuery = `
    INSERT IGNORE INTO trainees (
      user_id, entrepreneur_id, division_id, financial_year_id, annual_action_plan_id, training_title_id, payment_history_id, 
      certificate_status, certificate_generated_at, file_name, file_path, created_at, updated_at, deleted_at, old_data_ref_id, 
      old_data_ref_entrepreneur_id, old_data_ref_tc_id, trainee_email, certificate_code
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

      // Prepare the values to insert (in the same order as the columns in your insert query)
      const values = [
        newUserId,
        newEntUId,
        traineeData.division_id,
        traineeData.financial_year_id,
        traineeData.annual_action_plan_id,
        traineeData.training_title_id,
        traineeData.payment_history_id,
        traineeData.certificate_status,
        traineeData.certificate_generated_at,
        traineeData.file_name,
        traineeData.file_path,
        traineeData.created_at,
        traineeData.updated_at,
        traineeData.deleted_at,
        traineeData.old_data_ref_id,
        traineeData.old_data_ref_entrepreneur_id,
        traineeData.old_data_ref_tc_id,
        traineeData.trainee_email,
        traineeData.certificate_code,
      ];

      // Insert data using parameterized query
      const [traineeInsertResult] = await db1Connection.query(
        traineeInsertDataQuery,
        values
      );

      // Log the inserted ID
      console.log("Trainee inserted with ID: ", traineeInsertResult?.insertId);

      resolve(traineeInsertResult?.insertId);
    } catch (err) {
      reject(err?.message || err);
    }
  });
};

const compareDatabases = async () => {
  const db1Connection = await mysql.createConnection(db1Config);
  const db2Connection = await mysql.createConnection(db2Config);

  try {
    await syncMissingMSMEData(db1Connection, db2Connection);
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    await db1Connection.end();
    await db2Connection.end();
  }
};

compareDatabases().catch((err) => console.error(err));
