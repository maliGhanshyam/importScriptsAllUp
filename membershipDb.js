const mariadb = require("mariadb");
const { table } = require("table");

// ======================
// GLOBAL CONFIGURATION
// ======================
const DB_CONFIG = {
  host: "127.0.0.1",
  port: 3306,
  user: "root",
  password: "password",
  database: "testScriptDb",
  connectionLimit: 1,
};

// Batch identifiers
const BATCH_CONFIG = {
  MEMBERSHIP_GROUP_BATCH: "UFCMBZ_MembershipPlanGroup_09APR25_1",
  SINGLE_MEMBERSHIP_BATCH: "UFCMBZ_SingleMembershipPlan_09APR25_1",
  PAYMENT_PLAN_BATCH: "UFCMBZ_PaymentPlan_09APR25_1",
};

// Default values
const DEFAULT_VALUES = {
  GYM_ID: "dcd01f7c-9d1c-4ed1-93b3-aa845686eb4b",
  ADMIN_ID: 1,
  COUNTRY_ID: "927d1693-a1e4-4b1e-84a0-81a62f30c565",
  CURRENCY: "AED",
};

class MembershipMigrationManager {
  constructor(config) {
    this.pool = mariadb.createPool(config);
  }

  /**
   * Execute a SQL query
   * @param {string} sql - SQL query to execute
   * @param {Array} params - Query parameters
   * @returns {Promise<Array>} - Result of the query
   */
  async query(sql, params = []) {
    let conn;
    try {
      conn = await this.pool.getConnection();
      const rows = await conn.query(sql, params);
      return rows;
    } catch (err) {
      console.error("Error executing query:", err);
      throw err;
    } finally {
      if (conn) conn.release();
    }
  }

  /**
   * Create all necessary tables for membership migration
   */
  async createMembershipTables() {
    try {
      // Create MembershipPlanGroup table
      await this.query(`
                CREATE TABLE IF NOT EXISTS MembershipPlanGroup (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description VARCHAR(255),
                    adminId VARCHAR(250),
                    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    gymId VARCHAR(250),
                    batchNo VARCHAR(250),
                    externalId TEXT NULL
                )
            `);

      // Create SingleMembershipPlan table
      await this.query(`
                CREATE TABLE IF NOT EXISTS SingleMembershipPlan (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL,
                    color VARCHAR(255),
                    status VARCHAR(255) NOT NULL,
                    trial BOOLEAN DEFAULT false NOT NULL,
                    visible BOOLEAN NOT NULL,
                    targetMinAge INT NULL,
                    targetMaxAge INT NULL,
                    adminId TEXT NOT NULL,
                    membershipPlanGroupId INT NOT NULL,
                    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    targetGender VARCHAR(255),
                    gracePeriodCancellation INT DEFAULT 0 NULL,
                    gracePeriodChange INT DEFAULT 0 NULL,
                    gracePeriodEarlyRenewal INT DEFAULT 0 NULL,
                    gracePeriodRelocation INT DEFAULT 0 NULL,
                    gracePeriodTransfer INT DEFAULT 0 NULL,
                    championType VARCHAR(255),
                    isChampion BOOLEAN DEFAULT false NULL,
                    champAppId TEXT,
                    subDomesticGymIds TEXT,
                    batchNo TEXT,
                    externalId TEXT NULL,
                    gracePeriodMembershipExtension VARCHAR(255),
                    FOREIGN KEY (membershipPlanGroupId) REFERENCES MembershipPlanGroup(id)
                )
            `);

      // Create PaymentPlan table
      await this.query(`
                CREATE TABLE IF NOT EXISTS PaymentPlan (
                    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(255) NULL,
                    price DOUBLE DEFAULT 0 NOT NULL,
                    joiningFee DOUBLE DEFAULT 0 NOT NULL,
                    currency TEXT NULL,
                    paymentRecursionType VARCHAR(100) NULL,
                    paymentRecursionDuration INT NULL,
                    sessionPackPlanId VARCHAR(255) NULL,
                    singleMembershipPlanId VARCHAR(255) NOT NULL,
                    groupMembershipPlanId VARCHAR(255) NULL,
                    adminId VARCHAR(255) NOT NULL,
                    countryId VARCHAR(255) NOT NULL,
                    createdAt TIMESTAMP NULL,
                    updatedAt TIMESTAMP NULL,
                    hasEndDate VARCHAR(50) NULL,
                    recursionDuration INT NULL,
                    recursionPeriod VARCHAR(50) NULL,
                    chargeOnFirst BOOLEAN DEFAULT false NOT NULL,
                    giftPeriodFree BOOLEAN DEFAULT false NOT NULL,
                    gracePeriodDays INT DEFAULT 0 NOT NULL,
                    allowFreeze BOOLEAN DEFAULT false NOT NULL,
                    installmentAmount DOUBLE DEFAULT 0 NULL,
                    installmentFrequencyType VARCHAR(50) NULL,
                    installmentRounds INT DEFAULT 0 NULL,
                    surcharge DOUBLE DEFAULT 0 NULL,
                    status VARCHAR(50) DEFAULT 'active' NULL,
                    batchNo TEXT NULL,
                    allDays BOOLEAN DEFAULT true NULL,
                    days JSON DEFAULT '[]' NULL,
                    \`interval\` VARCHAR(50) DEFAULT 'day' NULL,
                    numberOfCheckIns INT DEFAULT 0 NULL,
                    unlimited BOOLEAN DEFAULT true NULL,
                    bringGymBuddy BOOLEAN DEFAULT false NULL,
                    freeOfCharge BOOLEAN DEFAULT true NULL,
                    gymBuddyInterval VARCHAR(50) DEFAULT 'day' NULL,
                    gymBuddyPrice FLOAT DEFAULT 0 NULL,
                    numberOfTimes INT DEFAULT 0 NULL,
                    customBilling FLOAT NULL,
                    isOneTimeTrial BOOLEAN DEFAULT false NOT NULL
                )
            `);

      console.log("Membership tables created successfully");
    } catch (err) {
      console.error("Error creating membership tables:", err);
      throw err;
    }
  }

  /**
   * Migrate membership data from source tables
   */
  async migrateMembershipData() {
    let conn;
    try {
      conn = await this.pool.getConnection();
      await conn.beginTransaction();

      // 1. Migrate MembershipPlanGroup
      const groupResult = await conn.query(
        `
                INSERT INTO MembershipPlanGroup (name, description, batchNo)
                SELECT DISTINCT category, category, ?
                FROM membership 
                WHERE category IS NOT NULL
            `,
        [BATCH_CONFIG.MEMBERSHIP_GROUP_BATCH]
      );

      console.log(
        `Inserted ${groupResult.affectedRows} records into MembershipPlanGroup`
      );

      // Update gymId for all groups
      await conn.query(
        `
                UPDATE MembershipPlanGroup  
                SET gymId = ?, adminId = ?
                WHERE gymId IS NULL
            `,
        [DEFAULT_VALUES.GYM_ID, DEFAULT_VALUES.ADMIN_ID]
      );

      // 2. Migrate SingleMembershipPlan
      const membershipResult = await conn.query(
        `
                INSERT INTO SingleMembershipPlan (
                    name, description, color, status, trial, visible, 
                    targetMinAge, targetMaxAge, adminId, membershipPlanGroupId, 
                    createdAt, updatedAt, targetGender, gracePeriodCancellation, 
                    gracePeriodChange, gracePeriodEarlyRenewal, gracePeriodRelocation, 
                    gracePeriodTransfer, championType, isChampion, champAppId, 
                    subDomesticGymIds, batchNo, gracePeriodMembershipExtension
                )
                SELECT 
                    m.membership AS name,
                    m.membership AS description,
                    NULL AS color,
                    IF(m.isActive = 1, 'Active', 'Inactive') AS status,
                    0 AS trial,
                    CASE WHEN m.isActive IS NOT NULL THEN TRUE ELSE FALSE END AS visible,
                    4 AS targetMinAge,
                    91 AS targetMaxAge,
                    ? AS adminId,
                    mpg.id AS membershipPlanGroupId,
                    CURRENT_TIMESTAMP AS createdAt,
                    CURRENT_TIMESTAMP AS updatedAt,
                    'any' AS targetGender,
                    m.period AS gracePeriodCancellation,
                    m.period AS gracePeriodChange,
                    m.period AS gracePeriodEarlyRenewal,
                    m.period AS gracePeriodRelocation,
                    m.period AS gracePeriodTransfer,
                    NULL AS championType,
                    CASE WHEN LOWER(m.category) = 'champion' THEN TRUE ELSE FALSE END AS isChampion,
                    NULL AS champAppId,
                    NULL AS subDomesticGymIds,
                    ? AS batchNo,
                    NULL AS gracePeriodMembershipExtension
                FROM membership m
                JOIN MembershipPlanGroup mpg ON m.category = mpg.name
            `,
        [DEFAULT_VALUES.ADMIN_ID, BATCH_CONFIG.SINGLE_MEMBERSHIP_BATCH]
      );

      console.log(
        `Inserted ${membershipResult.affectedRows} records into SingleMembershipPlan`
      );

      // 3. Migrate PaymentPlan
      const paymentResult = await conn.query(
        `
                INSERT INTO PaymentPlan (
                    name, price, singleMembershipPlanId, adminId, countryId, 
                    currency, type, createdAt, updatedAt, batchNo
                )
                SELECT 
                    TRIM(SUBSTRING_INDEX(
                        SUBSTRING_INDEX(m.prices, 'Price Name = ', -1),
                        ', Price Value =',
                        1
                    )) AS name,
                    CAST(
                        TRIM(SUBSTRING_INDEX(
                            SUBSTRING_INDEX(m.prices, 'Price Value = ', -1),
                            ',',
                            1
                        )) AS DECIMAL(10,2)
                    ) AS price,
                    s.id AS singleMembershipPlanId,
                    ? AS adminId,
                    ? AS countryId,
                    ? AS currency,
                    m.type AS type, 
                    NOW() AS createdAt,
                    NOW() AS updatedAt,
                    ? AS batchNo
                FROM membership m
                JOIN SingleMembershipPlan s ON m.membership = s.name
                WHERE m.prices LIKE '%Price Name = %' 
                AND m.prices LIKE '%Price Value = %'
            `,
        [
          DEFAULT_VALUES.ADMIN_ID,
          DEFAULT_VALUES.COUNTRY_ID,
          DEFAULT_VALUES.CURRENCY,
          BATCH_CONFIG.PAYMENT_PLAN_BATCH,
        ]
      );

      console.log(
        `Inserted ${paymentResult.affectedRows} records into PaymentPlan`
      );

      await conn.commit();
      return {
        groups: groupResult.affectedRows,
        memberships: membershipResult.affectedRows,
        paymentPlans: paymentResult.affectedRows,
      };
    } catch (err) {
      if (conn) await conn.rollback();
      console.error("Error migrating membership data:", err);
      throw err;
    } finally {
      if (conn) conn.release();
    }
  }

  /**
   * Display table data in a formatted way
   * @param {string} tableName - Name of the table to display
   */
  async displayTable(tableName, limit = 10) {
    try {
      const data = await this.query(`SELECT * FROM ${tableName} LIMIT ?`, [
        limit,
      ]);
      if (data.length === 0) {
        console.log(`Table ${tableName} is empty.`);
        return;
      }

      // Prepare table headers
      const headers = Object.keys(data[0]);
      const tableData = [headers];

      // Prepare table rows
      data.forEach((row) => {
        tableData.push(Object.values(row));
      });

      // Display the table
      console.log(table(tableData));
    } catch (err) {
      console.error("Error displaying table:", err);
    }
  }

  /**
   * Close the connection pool
   */
  async close() {
    await this.pool.end();
  }
}

// Example usage
(async () => {
  const migrationManager = new MembershipMigrationManager(DB_CONFIG);

  try {
    // Step 1: Create all membership tables
    await migrationManager.createMembershipTables();

    // Step 2: Migrate membership data
    const results = await migrationManager.migrateMembershipData();
    console.log("Migration results:", results);

    // Display some tables for verification
    console.log("\nMembershipPlanGroup table:");
    await migrationManager.displayTable("MembershipPlanGroup");

    console.log("\nSingleMembershipPlan table:");
    await migrationManager.displayTable("SingleMembershipPlan");

    console.log("\nPaymentPlan table:");
    await migrationManager.displayTable("PaymentPlan");
  } catch (err) {
    console.error("Error in membership migration:", err);
  } finally {
    await migrationManager.close();
  }
})();
