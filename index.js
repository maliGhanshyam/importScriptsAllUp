const mariadb = require('mariadb');
const { table } = require('table');

// ======================
// GLOBAL CONFIGURATION
// ======================
const DB_CONFIG = {
    host: '127.0.0.1',
    port: 3306,
    user: 'root',
    password: 'password',
    database: 'membersDb',
    connectionLimit: 5
};

// Batch identifiers
const BATCH_CONFIG = {
    CUSTOMERS_BATCH: 'UFCMBZ_customers_14APR25_1',
    LEADS_BATCH: 'UFCMBZ_leads_14APR25_1'
};

// Default values
const DEFAULT_VALUES = {
    GYM_ID: 'dcd01f7c-9d1c-4ed1-93b3-aa845686eb4b',
};


class MariaDBDockerManager {
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
            console.error('Error executing query:', err);
            throw err;
        } finally {
            if (conn) conn.release();
        }
    }

    /**
     * Create all necessary tables
     */
    async createTables() {
        try {
            // Create admins table
            await this.query(`
                CREATE TABLE IF NOT EXISTS admins (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255),
                    email VARCHAR(255) NOT NULL,
                    password VARCHAR(255),
                    contact_number VARCHAR(20),
                    admin_type VARCHAR(50) DEFAULT 'GLOBAL',
                    gender ENUM('MALE', 'FEMALE', 'OTHER'),
                    auth_id VARCHAR(255),
                    status VARCHAR(50) NOT NULL,
                    created_by INT NOT NULL,
                    last_updated_by INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
                    is_admin BOOLEAN DEFAULT TRUE NOT NULL,
                    is_instructor BOOLEAN DEFAULT FALSE NOT NULL,
                    instructor_id INT,
                    photo VARCHAR(255),
                    batch_no VARCHAR(100),
                    external_id VARCHAR(100),
                    external_username VARCHAR(100),
                    is_sys_admin BOOLEAN DEFAULT FALSE NOT NULL,
                    use_for_signup BOOLEAN DEFAULT FALSE NOT NULL
                )
            `);

            // Create countries table
            await this.query(`
                CREATE TABLE IF NOT EXISTS countries (
                    id INT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    iso_code VARCHAR(10) NOT NULL,
                    dial_code VARCHAR(10) NOT NULL,
                    flag_photo VARCHAR(255),
                    vat DOUBLE DEFAULT 0,
                    service_phone_number VARCHAR(20) NOT NULL,
                    time_zone_identifier VARCHAR(100),
                    vat_id VARCHAR(100),
                    p_key_customers INT AUTO_INCREMENT PRIMARY KEY,
                    currency_name VARCHAR(100),
                    currency_code VARCHAR(10),
                    currency_symbol VARCHAR(10),
                    currency_decimal_place INT,
                    currency_loweset_denomination DOUBLE,
                    currency_sub_unit_name VARCHAR(100),
                    UNIQUE (id)
                )
            `);

            // Create country_cities table
            await this.query(`
                CREATE TABLE IF NOT EXISTS country_cities (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    country_id INT NOT NULL,
                    FOREIGN KEY (country_id) REFERENCES countries(id)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE
                )
            `);

            // Create customers table
            await this.query(`
                CREATE TABLE IF NOT EXISTS customers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    is_email_verified BOOLEAN DEFAULT FALSE NOT NULL,
                    contact_number VARCHAR(20),
                    is_phone_verified BOOLEAN DEFAULT FALSE NOT NULL,
                    accessed_by_mobile BOOLEAN DEFAULT FALSE NOT NULL,
                    auth_id VARCHAR(255),
                    country_id INT,
                    status ENUM('ACTIVE', 'INACTIVE') DEFAULT 'ACTIVE' NOT NULL,
                    gender ENUM('MALE', 'FEMALE', 'RATHER_NOT_SAY'),
                    photo VARCHAR(255),
                    dob DATE,
                    pt_pref JSON DEFAULT JSON_ARRAY(),
                    gym_class_pref JSON DEFAULT JSON_ARRAY(),
                    gym_pref JSON DEFAULT JSON_ARRAY(),
                    created_by INT,
                    last_updated_by INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
                    is_parent BOOLEAN DEFAULT FALSE,
                    is_linked_account BOOLEAN DEFAULT FALSE,
                    parent_customer_id INT,
                    lead_id VARCHAR(255),
                    linked_account_id VARCHAR(255),
                    middle_name VARCHAR(255),
                    x_app_source VARCHAR(100),
                    x_app_version VARCHAR(50),
                    x_app_os VARCHAR(50),
                    city_id INT,
                    description TEXT,
                    age INT,
                    online_status ENUM('ONLINE', 'OFFLINE') DEFAULT 'ONLINE' NOT NULL,
                    height INT,
                    weight DECIMAL(16,3),
                    training_place VARCHAR(255),
                    location POINT,
                    location_name VARCHAR(255),
                    distance INT,
                    background_image VARCHAR(255),
                    interests TEXT,
                    social_accounts JSON DEFAULT JSON_ARRAY(),
                    height_unit VARCHAR(10),
                    weight_unit VARCHAR(10),
                    parq_health JSON DEFAULT JSON_ARRAY(),
                    gfp_health JSON DEFAULT JSON_ARRAY(),
                    t_and_c_signature_link VARCHAR(255),
                    external_id VARCHAR(255),
                    batch_no VARCHAR(255),
                    champ_id VARCHAR(255),
                    FOREIGN KEY (city_id) REFERENCES country_cities(id),
                    FOREIGN KEY (country_id) REFERENCES countries(id)
                    ON DELETE CASCADE ON UPDATE CASCADE
                )
            `);

            // Create leads table
            await this.query(`
                CREATE TABLE IF NOT EXISTS leads (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    created_by INT UNSIGNED NULL,
                    last_updated_by INT UNSIGNED NULL,
                    vendor_id INT UNSIGNED NULL,
                    vendor_type INT UNSIGNED NULL,
                    company_id INT UNSIGNED NULL,
                    lead_created_by VARCHAR(255) NULL,
                    gym_id VARCHAR(255) NULL,
                    first_name VARCHAR(255) NOT NULL,
                    middle_name VARCHAR(255) NULL,
                    last_name VARCHAR(255) NULL,
                    description VARCHAR(255) NULL,
                    dob DATE NULL,
                    nationality VARCHAR(255) NULL,
                    gender VARCHAR(50) DEFAULT 'ACTIVE',
                    address VARCHAR(255) NULL,
                    photo VARCHAR(255) NULL,
                    email VARCHAR(255) NULL,
                    \`source\` VARCHAR(255) NULL,
                    is_email_verified BOOLEAN DEFAULT FALSE NOT NULL,
                    phone_number VARCHAR(20) NULL,
                    lead_type ENUM('type1', 'type2', 'type3') NULL,
                    lead_status ENUM('status1', 'status2', 'status3') NULL,
                    status ENUM('ACTIVE', 'INACTIVE') DEFAULT 'ACTIVE' NOT NULL,
                    lead_status_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    work_status ENUM('status1', 'status2') NULL,
                    job_title VARCHAR(255) NULL,
                    marketing_consent_sms BOOLEAN DEFAULT FALSE NOT NULL,
                    marketing_consent_email BOOLEAN DEFAULT FALSE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
                    country_id INT UNSIGNED NULL,
                    city_id INT UNSIGNED NULL,
                    customer_id INT,
                    is_member BOOLEAN DEFAULT FALSE NOT NULL,
                    is_recurring BOOLEAN DEFAULT FALSE NOT NULL,
                    membership_details JSON NULL,
                    membership_status ENUM('status1', 'status2') NULL,
                    membership_end_date TIMESTAMP NULL,
                    parq_health JSON NULL,
                    gfp_health JSON NULL,
                    membership_action_date TIMESTAMP NULL,
                    is_parent BOOLEAN DEFAULT TRUE NOT NULL,
                    lead_id INT UNSIGNED NULL,
                    t_and_c_signature_link VARCHAR(255) NULL,
                    nick_name VARCHAR(255) NULL,
                    external_id VARCHAR(255) NULL,
                    batch_no VARCHAR(255) NULL,
                    lead_no INT NULL,
                    otp VARCHAR(10) NULL,
                    last_otp_creation_datetime TIMESTAMP NULL,
                    otp_count INT NULL,
                    external_due_amount INT NULL,
                    external_paid_amount INT NULL,
                    external_membership_code VARCHAR(255) NULL,
                    customer_code VARCHAR(255) NULL,
                    is_class_booking_blocked BOOLEAN DEFAULT FALSE NOT NULL,
                    date_of_class_block TIMESTAMP NULL,
                    date_of_last_class_unblocked TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    lead_sub_status ENUM('status1', 'status2') NULL,
                    lead_referal_status ENUM('status1', 'status2') NULL,
                    first_signup BOOLEAN DEFAULT FALSE NOT NULL,
                    referred_by_id VARCHAR(255) NULL,
                    referred_by_name VARCHAR(255) NULL,
                    payment_status ENUM('status1', 'status2') NULL,
                    subscribed_instructors JSON NULL,
                    marked_as_delete BOOLEAN DEFAULT FALSE NOT NULL,
                    champ_id VARCHAR(255) NULL,
                    landing_page_name VARCHAR(255) NULL,
                    landing_page_offer VARCHAR(255) NULL,
                    lead_trainer_id VARCHAR(255) NULL,
                    old_lead_status ENUM('status1', 'status2') NULL,
                    sign_up_statuses JSON NULL,
                    search_vector TEXT NULL,
                    UNIQUE KEY uq_lead_no (lead_no),
                    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE ON UPDATE CASCADE
                )
            `);

            console.log('All tables created successfully');
        } catch (err) {
            console.error('Error creating tables:', err);
            throw err;
        }
    }

    /**
     * Initialize with sample data
     */
    async initializeSampleData() {
        try {
            // Insert admin data
            await this.query(`
                INSERT INTO admins (
                    first_name,
                    last_name,
                    email,
                    status,
                    created_by,
                    last_updated_by
                ) VALUES (
                    'Import',
                    'Team',
                    'team.import@gmail.com',
                    'ACTIVE',
                    1,
                    1
                )
            `);

            // Insert country data
            await this.query(`
                INSERT INTO countries(  
                    id,
                    name,
                    status,
                    iso_code,
                    dial_code,
                    flag_photo,
                    vat,
                    service_phone_number,
                    time_zone_identifier,
                    vat_id,
                    currency_name,
                    currency_code,
                    currency_symbol,
                    currency_decimal_place,
                    currency_loweset_denomination,
                    currency_sub_unit_name
                ) VALUES (
                    1,
                    'United Arab Emirates',
                    'ACTIVE',
                    'AE',
                    '+971',
                    'https://gms-public-assets.s3-eu-west-1.amazonaws.com/flags/png250px/ae.png',
                    5,
                    '+971',
                    'Asia/Dubai',
                    '1SEDF',
                    'AED',
                    'AED',
                    'AE',
                    4,
                    0.1,
                    'Phil'
                )
            `);

            // Insert city data
            await this.query(`
                INSERT INTO country_cities (name, country_id) VALUES ('Dubai', 1)
            `);

            console.log('Sample data inserted successfully');
        } catch (err) {
            console.error('Error inserting sample data:', err);
            throw err;
        }
    }

    /**
     * Migrate data from member table to customers table
     */
    async migrateMemberToCustomers() {
        try {
            const result = await this.query(`
                INSERT INTO customers (
                    first_name,
                    last_name,
                    email,
                    contact_number,
                    country_id,
                    status,
                    gender,
                    dob,
                    batch_no
                )
                SELECT
                    SUBSTRING_INDEX(m.member, ' ', 1) AS first_name,
                    SUBSTRING_INDEX(m.member, ' ', -1) AS last_name,
                    m.email,
                    m.phone,
                    CASE 
                        WHEN m.nationality = 'United Arab Emirates' THEN 1
                        ELSE NULL
                    END AS country_id,
                    CASE 
                        WHEN m.status = 'Active' THEN 'ACTIVE'
                        ELSE 'INACTIVE'
                    END AS status,
                    CASE 
                        WHEN m.gender = 'M' THEN 'MALE'
                        WHEN m.gender = 'F' THEN 'FEMALE'
                        ELSE NULL
                    END AS gender,
                    m.birthDay,
                    '${BATCH_CONFIG.CUSTOMERS_BATCH}' AS batch_no
                FROM
                    member m
            `);
            
            console.log(`Migrated ${result.affectedRows} records from member to customers`);
            return result;
        } catch (err) {
            console.error('Error migrating member data:', err);
            throw err;
        }
    }

    /**
     * Migrate data from imported_leads to leads table
     */
    async migrateImportedLeads() {
        try {
            // First insert the leads
            const insertResult = await this.query(`
                INSERT INTO leads (first_name, last_name, email, phone_number, nationality, source, batch_no)
                SELECT 
                    SUBSTRING_INDEX(name, ' ', 1) AS first_name, 
                    TRIM(SUBSTRING(name, LOCATE(' ', name) + 1)) AS last_name,
                    emailAddress AS email,
                    mobileNumber AS phone_number,
                    nationality,
                    leadSource AS source,
                    '${BATCH_CONFIG.LEADS_BATCH}' AS batch_no
                FROM imported_leads
            `);

            // Update gym_id for leads
            await this.query(`
                UPDATE leads  
               SET gym_id = '${DEFAULT_VALUES.GYM_ID}' 
                WHERE gym_id IS NULL
            `);

            // Update customer_id by joining with customers
            const updateResult = await this.query(`
                UPDATE leads lc
                INNER JOIN customers c
                    ON lc.phone_number = c.contact_number
                SET lc.customer_id = c.id
            `);

            console.log(`Migrated ${insertResult.affectedRows} leads and updated ${updateResult.affectedRows} with customer IDs`);
            return { insertResult, updateResult };
        } catch (err) {
            console.error('Error migrating leads data:', err);
            throw err;
        }
    }

    /**
     * Find unique sales persons not in admins
     */
    async findUniqueSalesPersons() {
        try {
            const result = await this.query(`
                SELECT name 
                FROM (
                    SELECT SUBSTRING_INDEX(salesPerson, ' ', 1) AS name FROM imported_leads
                    UNION
                    SELECT SUBSTRING_INDEX(sales, ' ', 1) AS name FROM member
                ) AS combined
                WHERE name NOT IN (SELECT first_name FROM admins)
            `);
            
            console.log('Unique sales persons not in admins:', result);
            return result;
        } catch (err) {
            console.error('Error finding unique sales persons:', err);
            throw err;
        }
    }

    /**
     * Display table data in a formatted way
     * @param {string} tableName - Name of the table to display
     */
    async displayTable(tableName, limit = 10) {
        try {
            const data = await this.query(`SELECT * FROM ${tableName} LIMIT ?`, [limit]);
            if (data.length === 0) {
                console.log(`Table ${tableName} is empty.`);
                return;
            }

            // Prepare table headers
            const headers = Object.keys(data[0]);
            const tableData = [headers];

            // Prepare table rows
            data.forEach(row => {
                tableData.push(Object.values(row));
            });

            // Display the table
            console.log(table(tableData));
        } catch (err) {
            console.error('Error displaying table:', err);
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

    const dbManager = new MariaDBDockerManager(DB_CONFIG);

    try {
        // Step 1: Create all tables
        await dbManager.createTables();

        // Step 2: Initialize with sample data
        await dbManager.initializeSampleData();

        // Step 3: Migrate data from member to customers
        await dbManager.migrateMemberToCustomers();

        // Step 4: Migrate data from imported_leads to leads
        await dbManager.migrateImportedLeads();

        // Step 5: Find unique sales persons not in admins
        await dbManager.findUniqueSalesPersons();

        // Display some tables for verification
        console.log('\nAdmins table:');
        await dbManager.displayTable('admins');

        console.log('\nCustomers table (first 10 rows):');
        await dbManager.displayTable('customers');

        console.log('\nLeads table (first 10 rows):');
        await dbManager.displayTable('leads');

    } catch (err) {
        console.error('Error in operations:', err);
    } finally {
        await dbManager.close();
    }
})();