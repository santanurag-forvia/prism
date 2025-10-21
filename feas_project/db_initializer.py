"""
feas_project/db_initializer.py

Class-based DB initializer for FEAS using mysql.connector (same style as your reference).
Idempotent; creates tables in dependency-safe order and seeds roles.
"""

import os
import sys
import traceback
from typing import Dict, Tuple

import mysql.connector
from mysql.connector import errorcode, Error

try:
    import django
    from django.conf import settings

    if not settings.configured:
        dj_settings_module = os.getenv("DJANGO_SETTINGS_MODULE")
        if not dj_settings_module:
            raise RuntimeError(
                "DJANGO_SETTINGS_MODULE not set. Set it or call this from inside Django."
            )
        django.setup()
except Exception:
    settings = None  # allow import even if running standalone


class DatabaseInitializer:
    INIT_KEY = "db_initialized"
    DEFAULT_INIT_TABLE = "system_settings"

    def __init__(self, db_config: Dict = None):
        if db_config:
            self.db_config = db_config
        else:
            self.db_config = self._get_db_config_from_settings()

        self.init_table = (
            getattr(settings, "DB_INIT_DONE_TABLE", self.DEFAULT_INIT_TABLE)
            if settings is not None
            else self.DEFAULT_INIT_TABLE
        )

        # Build DDLs in dependency-aware order
        self.ddl_statements = self._build_ddls(self.init_table)

        self.role_inserts = [
            ("ADMIN", "Administrator"),
            ("PDL", "Program Development Lead"),
            ("COE_LEADER", "COE Leader"),
            ("TEAM_LEAD", "Team Lead"),
            ("EMPLOYEE", "Employee"),
        ]

    def _get_db_config_from_settings(self) -> Dict:
        if settings is None:
            raise RuntimeError(
                "Django settings are not available. Set DJANGO_SETTINGS_MODULE or call from inside Django."
            )

        dbs = settings.DATABASES.get("default", {})
        cfg = {
            "host": dbs.get("HOST", "127.0.0.1") or "127.0.0.1",
            "port": int(dbs.get("PORT", 3306) or 3306),
            "user": dbs.get("USER", "root") or "",
            "password": dbs.get("PASSWORD", "root") or "",
            "database": dbs.get("NAME", "feasdb") or "",
            "charset": "utf8mb4",
            "use_unicode": True,
        }
        return cfg

    def _build_ddls(self, init_table_name: str) -> Tuple[str, ...]:
        """
        Build DDLs in an order that respects foreign-key dependencies.
        Order summary (top-down):
         1. init table (system_settings)
         2. lookup tables (roles)
         3. users and LDAP-related tables
         4. projects & prism_wbs & subprojects (core project metadata)
         5. COEs, domains (which reference users/projects)
         6. project link tables (project_coes, project_contacts)
         7. monthly/prism/holidays/monthly_hours_limit
         8. allocations and allocation_items (depends on users/projects)
         9. monthly_allocation_entries then weekly_allocations (weekly -> mae)
        10. notifications, audit_log, ldap_sync_jobs
        """
        ddls = []

        # 1) Initialization table (stores init flag)
        print(f"Adding DDL for table: {init_table_name}")
        ddls.append(f"""
            CREATE TABLE IF NOT EXISTS `{init_table_name}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `key_name` VARCHAR(128) NOT NULL UNIQUE,
                `value_text` TEXT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 2) Roles (lookup) - no dependencies
        print("Adding DDL for table: roles")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `roles` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `role_key` VARCHAR(64) NOT NULL UNIQUE,
                `display_name` VARCHAR(128) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 3) Users & LDAP tables (users used by many others)
        print("Adding DDL for table: users")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `users` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `username` VARCHAR(150) NOT NULL UNIQUE,
                `email` VARCHAR(254),
                `ldap_id` VARCHAR(255) UNIQUE,
                `role` VARCHAR(32) NOT NULL DEFAULT 'EMPLOYEE',
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Adding DDL for table: ldap_directory")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `ldap_directory` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `username` VARCHAR(150) NOT NULL,
                `email` VARCHAR(254),
                `cn` VARCHAR(255),
                `givenName` VARCHAR(150),
                `sn` VARCHAR(150),
                `title` VARCHAR(255),
                `department` VARCHAR(255),
                `telephoneNumber` VARCHAR(64),
                `mobile` VARCHAR(64),
                `manager_dn` VARCHAR(512),
                `ldap_dn` VARCHAR(1024) NOT NULL,
                `ldap_dn_hash` CHAR(64) NOT NULL,
                `attributes_json` JSON NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_ldap_directory_dn_hash` (`ldap_dn_hash`),
                UNIQUE KEY `uq_ldap_directory_username` (`username`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Adding DDL for table: ldap_sync_history")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `ldap_sync_history` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `synced_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `synced_by` VARCHAR(255),
                `details` TEXT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Adding DDL for table: ldap_sync_jobs")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `ldap_sync_jobs` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `started_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `finished_at` TIMESTAMP NULL,
                `started_by` VARCHAR(255),
                `status` VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                `total_count` INT DEFAULT 0,
                `processed_count` INT DEFAULT 0,
                `errors_count` INT DEFAULT 0,
                `details` TEXT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 4) Projects and Prism WBS (projects referenced by many tables)
        print("Adding DDL for table: projects")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `projects` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `name` VARCHAR(255) NOT NULL,
                `oem_name` VARCHAR(255),
                `pdl_user_id` BIGINT,
                `pm_user_id` BIGINT,
                `start_date` DATE,
                `end_date` DATE,
                `description` TEXT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_project_name` (`name`),
                FOREIGN KEY (`pdl_user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL,
                FOREIGN KEY (`pm_user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Adding DDL for table: prism_wbs")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `prism_wbs` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `iom_id` VARCHAR(255) NOT NULL,
                `status` VARCHAR(64),
                `project_id` BIGINT,
                `bg_code` VARCHAR(128),
                `year` VARCHAR(16),
                `seller_country` VARCHAR(128),
                `creator` VARCHAR(255),
                `date_created` DATETIME,
                `comment_of_creator` TEXT,
                `buyer_bau` VARCHAR(255),
                `buyer_wbs_cc` VARCHAR(255),
                `seller_bau` VARCHAR(255),
                `seller_wbs_cc` VARCHAR(255),
                `site` VARCHAR(255),
                `function` VARCHAR(255),
                `department` VARCHAR(255),
                `jan_hours` DECIMAL(10,2) DEFAULT 0,
                `feb_hours` DECIMAL(10,2) DEFAULT 0,
                `mar_hours` DECIMAL(10,2) DEFAULT 0,
                `apr_hours` DECIMAL(10,2) DEFAULT 0,
                `may_hours` DECIMAL(10,2) DEFAULT 0,
                `jun_hours` DECIMAL(10,2) DEFAULT 0,
                `jul_hours` DECIMAL(10,2) DEFAULT 0,
                `aug_hours` DECIMAL(10,2) DEFAULT 0,
                `sep_hours` DECIMAL(10,2) DEFAULT 0,
                `oct_hours` DECIMAL(10,2) DEFAULT 0,
                `nov_hours` DECIMAL(10,2) DEFAULT 0,
                `dec_hours` DECIMAL(10,2) DEFAULT 0,
                `total_hours` DECIMAL(14,2) DEFAULT 0,
                `jan_fte` DECIMAL(8,4) DEFAULT 0,
                `feb_fte` DECIMAL(8,4) DEFAULT 0,
                `mar_fte` DECIMAL(8,4) DEFAULT 0,
                `apr_fte` DECIMAL(8,4) DEFAULT 0,
                `may_fte` DECIMAL(8,4) DEFAULT 0,
                `jun_fte` DECIMAL(8,4) DEFAULT 0,
                `jul_fte` DECIMAL(8,4) DEFAULT 0,
                `aug_fte` DECIMAL(8,4) DEFAULT 0,
                `sep_fte` DECIMAL(8,4) DEFAULT 0,
                `oct_fte` DECIMAL(8,4) DEFAULT 0,
                `nov_fte` DECIMAL(8,4) DEFAULT 0,
                `dec_fte` DECIMAL(8,4) DEFAULT 0,
                `total_fte` DECIMAL(12,4) DEFAULT 0,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_prism_wbs_iom` (`iom_id`),
                FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 5) Subprojects (depends on projects)
        print("Adding DDL for table: subprojects")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `subprojects` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `project_id` BIGINT NOT NULL,
                `name` VARCHAR(512) NOT NULL,
                `mdm_code` VARCHAR(128) DEFAULT NULL,
                `bg_code` VARCHAR(128) DEFAULT NULL,
                `mdm_code_norm` VARCHAR(128) GENERATED ALWAYS AS (UPPER(TRIM(COALESCE(`mdm_code`,'')))) STORED,
                `bg_code_norm`  VARCHAR(128) GENERATED ALWAYS AS (UPPER(TRIM(COALESCE(`bg_code`,'')))) STORED,
                `priority` INT DEFAULT 0,
                `description` TEXT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_subproject_project_name` (`project_id`, `name`),
                KEY `idx_subprojects_mdm_code_norm` (`mdm_code_norm`),
                KEY `idx_subprojects_bg_code_norm` (`bg_code_norm`),
                CONSTRAINT `fk_subproj_project` FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 6) COEs and Domains (COEs references users; domains references coes)
        print("Adding DDL for table: coes")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `coes` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `name` VARCHAR(255) NOT NULL UNIQUE,
                `leader_user_id` BIGINT,
                `description` TEXT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (`leader_user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Adding DDL for table: domains")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `domains` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `coe_id` BIGINT NOT NULL,
                `name` VARCHAR(255) NOT NULL,
                `lead_user_id` BIGINT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_domain_coe_name` (`coe_id`, `name`),
                FOREIGN KEY (`coe_id`) REFERENCES `coes`(`id`) ON DELETE CASCADE,
                FOREIGN KEY (`lead_user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 7) Project-level link tables (project_coes, project_contacts)
        print("Adding DDL for table: project_coes")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `project_coes` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `project_id` BIGINT NOT NULL,
                `coe_id` BIGINT NOT NULL,
                `assigned_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_project_coe` (`project_id`, `coe_id`),
                FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE CASCADE,
                FOREIGN KEY (`coe_id`) REFERENCES `coes`(`id`) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Adding DDL for table: project_contacts")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `project_contacts` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `project_id` BIGINT NOT NULL,
                `contact_type` VARCHAR(16) NOT NULL,
                `contact_name` VARCHAR(512),
                `user_id` BIGINT NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_proj_contact` (`project_id`,`contact_type`,`contact_name`),
                FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE CASCADE,
                FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 8) monthly_hours_limit and holidays (independent)
        print("Adding DDL for table: monthly_hours_limit")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS monthly_hours_limit (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                year SMALLINT NOT NULL,
                month TINYINT NOT NULL,
                start_date DATE NULL,
                end_date DATE NULL,
                max_hours DECIMAL(7,2) NOT NULL DEFAULT 183.75,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_year_month (year, month)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Prepping holidays table")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `holidays` (
              `id` BIGINT NOT NULL AUTO_INCREMENT,
              `holiday_date` DATE NOT NULL,
              `name` VARCHAR(255) NOT NULL,
              `created_by` VARCHAR(255) DEFAULT NULL,
              `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              UNIQUE KEY `uq_holiday_date` (`holiday_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        # 9) Allocations and allocation_items (allocations references users & projects)
        print("Adding DDL for table: allocations")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `allocations` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `user_id` BIGINT NOT NULL,
                `project_id` BIGINT NOT NULL,
                `month_start` DATE NOT NULL,
                `total_hours` INT UNSIGNED NOT NULL DEFAULT 0,
                `pending_hours` INT UNSIGNED NOT NULL DEFAULT 0,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_alloc_user_project_month` (`user_id`, `project_id`, `month_start`),
                FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
                FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Adding DDL for table: allocation_items")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `allocation_items` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `allocation_id` BIGINT NOT NULL,
                `project_id` BIGINT NOT NULL,
                `coe_id` BIGINT NOT NULL,
                `domain_id` BIGINT,
                `user_id` BIGINT,
                `user_ldap` VARCHAR(255) NOT NULL,
                `total_hours` INT UNSIGNED NOT NULL DEFAULT 0,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_alloc_item (allocation_id, coe_id, user_id),
                FOREIGN KEY (`allocation_id`) REFERENCES `allocations`(`id`) ON DELETE CASCADE,
                FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE CASCADE,
                FOREIGN KEY (`coe_id`) REFERENCES `coes`(`id`) ON DELETE CASCADE,
                FOREIGN KEY (`domain_id`) REFERENCES `domains`(`id`) ON DELETE SET NULL,
                FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 10) monthly_allocation_entries (depends on projects and prism_wbs)
        print("Adding DDL for table: monthly_allocation_entries")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `monthly_allocation_entries` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `project_id` BIGINT NULL,
            `subproject_id` BIGINT NULL,
            `iom_id` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
            `month_start` DATE NOT NULL,
            `user_ldap` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
            `total_hours` DECIMAL(10,2) UNSIGNED NOT NULL DEFAULT '0.00',
            `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `idx_project_id` (`project_id`),
            KEY `idx_subproject_id` (`subproject_id`),
            KEY `idx_iom_id` (`iom_id`),
            KEY `idx_month_start` (`month_start`),
            KEY `idx_proj_month` (`project_id`, `month_start`),
            KEY `idx_iom_month` (`iom_id`, `month_start`),
        
            CONSTRAINT `fk_monthly_alloc_project`
                FOREIGN KEY (`project_id`) REFERENCES `projects` (`id`)
                ON DELETE SET NULL ON UPDATE CASCADE,
        
            CONSTRAINT `fk_monthly_alloc_subproject`
                FOREIGN KEY (`subproject_id`) REFERENCES `subprojects` (`id`)
                ON DELETE SET NULL ON UPDATE CASCADE,
        
            CONSTRAINT `fk_monthly_alloc_iom`
                FOREIGN KEY (`iom_id`) REFERENCES `prism_wbs` (`iom_id`)
                ON DELETE SET NULL ON UPDATE CASCADE
        ) ENGINE=InnoDB
          DEFAULT CHARSET=utf8mb4
          COLLATE=utf8mb4_0900_ai_ci;

        """)
        # 12) team_distributions (recommended) - depends on monthly_allocation_entries optionally
        print("Adding DDL for table: team_distributions")
        ddls.append("""
                    CREATE TABLE IF NOT EXISTS `team_distributions` (
                        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                        `month_start` DATE NOT NULL,
                        `lead_ldap` VARCHAR(255) NOT NULL,
                        `project_id` BIGINT NULL,
                        `subproject_id` BIGINT NOT NULL,
                        `reportee_ldap` VARCHAR(255) NOT NULL,
                        `hours` DECIMAL(10,2) NOT NULL DEFAULT 0,
                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uq_lead_month_subp_reportee (lead_ldap, month_start, subproject_id, reportee_ldap),
                        INDEX idx_month_reportee (month_start, reportee_ldap),
                        INDEX idx_lead_month (lead_ldap, month_start)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                """)
        # 11) weekly_allocations (references monthly_allocation_entries.id)
        print("Adding DDL for table: weekly_allocations")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `weekly_allocations` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `allocation_id` BIGINT NULL,                -- existing monthly_allocation_entries link (nullable)
            `team_distribution_id` BIGINT NULL,         -- new link to team_distributions (nullable)
            `week_number` TINYINT NOT NULL,
            `hours` DECIMAL(10,2) UNSIGNED NOT NULL DEFAULT '0.00',
            `percent` DECIMAL(5,2) NOT NULL DEFAULT '0.00',
            `status` VARCHAR(16) NOT NULL DEFAULT 'PENDING',
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY `uq_week_alloc_allocation` (`allocation_id`, `week_number`),
            UNIQUE KEY `uq_week_alloc_team_dist` (`team_distribution_id`, `week_number`),
            CONSTRAINT `fk_weekly_alloc_mae`
                FOREIGN KEY (`allocation_id`) REFERENCES `monthly_allocation_entries` (`id`)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT `fk_weekly_alloc_team_dist`
                FOREIGN KEY (`team_distribution_id`) REFERENCES `team_distributions` (`id`)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

        """)

        # 13) Notifications and Audit (these reference users)
        print("Adding DDL for table: notifications")
        ddls.append("""CREATE TABLE weekly_punch_confirmations (
              id BIGINT AUTO_INCREMENT PRIMARY KEY,
              emp_email VARCHAR(128) NOT NULL,                              -- user email (unique id for employee)
              allocation_id BIGINT NOT NULL,                                -- now references team_distributions.id
              billing_start DATE NOT NULL,                                  -- canonical billing start for the month
              week_number INT NOT NULL,                                     -- 1..N relative to billing_start
              allocated_hours DECIMAL(8,2) NOT NULL,                       -- hours proposed for that week
              allocated_percent DECIMAL(6,2) DEFAULT NULL,                 -- optional percent
              user_comment VARCHAR(1024) DEFAULT NULL,                     -- optional comment from user on accept/reject
              status ENUM('PENDING','ACCEPTED','REJECTED','RECONSIDERED','CANCELLED') NOT NULL DEFAULT 'PENDING',
              tl_user_id VARCHAR(128) DEFAULT NULL,                         -- TL who will/has actioned the reconsideration
              tl_comment VARCHAR(2048) DEFAULT NULL,
              actioned_by VARCHAR(128) DEFAULT NULL,                        -- last actor (emp email or TL)
              actioned_at DATETIME DEFAULT NULL,
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              -- unique / index use emp_email (not emp_code)
              UNIQUE KEY uk_alloc_week (allocation_id, billing_start, week_number, emp_email),
              INDEX ix_emp_billing (emp_email, billing_start),
              CONSTRAINT fk_wpc_allocation_td FOREIGN KEY (allocation_id)
                REFERENCES team_distributions (id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        ddls.append("""CREATE TABLE weekly_punch_history (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          confirmation_id BIGINT NOT NULL,
          actor VARCHAR(128) NOT NULL,     -- emp_code or TL
          role VARCHAR(64) NOT NULL,       -- 'USER' or 'TL' or 'SYSTEM'
          action ENUM('ACCEPT','REJECT','TL_MODIFY','REASSIGN','CLOSE','REOPEN') NOT NULL,
          comment VARCHAR(2048) DEFAULT NULL,
          before_json JSON DEFAULT NULL,
          after_json JSON DEFAULT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (confirmation_id) REFERENCES weekly_punch_confirmations(id) ON DELETE CASCADE
        );""")
        # 13) Notifications and Audit (these reference users)
        print("Adding DDL for table: notifications")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `notifications` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `user_id` BIGINT NOT NULL,
                `message` TEXT NOT NULL,
                `is_read` TINYINT(1) NOT NULL DEFAULT 0,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        print("Adding DDL for table: audit_log")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `audit_log` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `user_id` BIGINT,
                `action` VARCHAR(255) NOT NULL,
                `meta` JSON,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 14) prism_master_wor_meta (low-dependency metadata)
        print("Adding DDL for table: prism_master_wor_meta")
        ddls.append("""
            CREATE TABLE IF NOT EXISTS `prism_master_wor_meta` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `table_name` VARCHAR(128) NOT NULL,
                `col_order` INT NOT NULL,
                `col_name` VARCHAR(255) NOT NULL,
                `orig_header` VARCHAR(1024),
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uq_prism_master_meta` (`table_name`,`col_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # Final summary print
        print(f"Total tables to create: {len(ddls)}")
        return tuple(ddls)

    def connect(self):
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except mysql.connector.Error:
            print("ERROR: Could not connect to MySQL with provided settings.")
            traceback.print_exc()
            raise

    def _execute_statements(self, conn, statements):
        cursor = conn.cursor()
        try:
            for sql in statements:
                s = sql.strip()
                if not s:
                    continue
                first_line = s.splitlines()[0][:160]
                print(f"Executing: {first_line} ...")
                cursor.execute(s)
            conn.commit()
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def _is_already_initialized(self, conn) -> bool:
        cursor = conn.cursor(dictionary=True)
        try:
            q = f"SELECT value_text FROM `{self.init_table}` WHERE key_name = %s LIMIT 1"
            cursor.execute(q, (self.INIT_KEY,))
            row = cursor.fetchone()
            if row and row.get("value_text") and str(row.get("value_text")).lower() in (
                "1",
                "true",
                "yes",
            ):
                return True
            return False
        finally:
            cursor.close()

    def _set_initialized_flag(self, conn):
        cursor = conn.cursor()
        try:
            q = f"""
            INSERT INTO `{self.init_table}` (key_name, value_text)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE value_text = VALUES(value_text), updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(q, (self.INIT_KEY, "true"))
            conn.commit()
        finally:
            cursor.close()

    def _seed_roles(self, conn):
        cursor = conn.cursor(dictionary=True)
        try:
            for key, display in self.role_inserts:
                cursor.execute("SELECT id FROM roles WHERE role_key = %s", (key,))
                if cursor.fetchone() is None:
                    print(f"Inserting role: {key}")
                    cursor.execute(
                        "INSERT INTO roles (role_key, display_name) VALUES (%s, %s)",
                        (key, display),
                    )
            conn.commit()
        finally:
            cursor.close()

    def initialize_database(self) -> bool:
        print("FEAS: Starting DB initialization...")
        conn = None
        try:
            conn = self.connect()
            # create init table first
            self._execute_statements(conn, [self.ddl_statements[0]])
            if self._is_already_initialized(conn):
                print("FEAS: Database already initialized. Skipping.")
                return True
            # create all other tables in the pre-determined safe order
            self._execute_statements(conn, list(self.ddl_statements[1:]))
            # seed roles
            self._seed_roles(conn)
            # set init flag
            self._set_initialized_flag(conn)
            print("FEAS: Database initialization completed successfully.")
            return True
        except mysql.connector.Error:
            print("FEAS: Database initialization failed due to MySQL error.")
            traceback.print_exc()
            return False
        except Exception:
            print("FEAS: Database initialization failed.")
            traceback.print_exc()
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


def initialize_database(db_config: Dict = None) -> bool:
    initializer = DatabaseInitializer(db_config=db_config) if db_config else DatabaseInitializer()
    return initializer.initialize_database()


if __name__ == "__main__":
    if "DJANGO_SETTINGS_MODULE" not in os.environ:
        print("Please set DJANGO_SETTINGS_MODULE to your settings module, e.g:")
        print("  export DJANGO_SETTINGS_MODULE=feas_project.settings")
        sys.exit(2)
    ok = initialize_database()
    if not ok:
        sys.exit(1)
    print("Done.")

def accept_week(request):
    user_email = request.session['ldap_username']
    data = json.loads(request.body)
    allocation_id = int(data['allocation_id'])
    billing_start = data['billing_start']
    week_number = int(data['week_number'])
    comment = data.get('comment','')
    # allocated_hours optional - if omitted pick existing weekly_allocations.hours or equal-split fallback
    allocated_hours = data.get('allocated_hours', None)

    sql = """
    START TRANSACTION;
    -- insert/update confirmation
    INSERT INTO weekly_punch_confirmations
    (user_email, allocation_id, billing_start, week_number, allocated_hours, user_comment, status, actioned_by, actioned_at)
    VALUES (%s, %s, %s, %s, COALESCE(%s,
      (SELECT COALESCE(wa.hours, TRUNCATE(m.total_hours / 4,2)) FROM monthly_allocation_entries m
         LEFT JOIN weekly_allocations wa ON wa.allocation_id = m.id AND wa.week_number = %s
         WHERE m.id = %s)
    ), %s, 'ACCEPTED', %s, NOW())
    ON DUPLICATE KEY UPDATE
      allocated_hours = VALUES(allocated_hours),
      user_comment = VALUES(user_comment),
      status = 'ACCEPTED',
      actioned_by = VALUES(actioned_by),
      actioned_at = VALUES(actioned_at),
      updated_at = NOW();

    -- upsert weekly_allocations using the accepted hours
    INSERT INTO weekly_allocations (allocation_id, week_number, hours, confirmed_by, confirmed_at)
    VALUES (%s, %s,
      (SELECT allocated_hours FROM weekly_punch_confirmations WHERE user_email=%s AND allocation_id=%s AND billing_start=%s AND week_number=%s LIMIT 1),
      %s, NOW())
    ON DUPLICATE KEY UPDATE hours = VALUES(hours), confirmed_by = VALUES(confirmed_by), confirmed_at = VALUES(confirmed_at);
    COMMIT;
    """
    params = [user_email, allocation_id, billing_start, week_number, allocated_hours, week_number, allocation_id, comment, user_email,
              allocation_id, week_number, user_email, allocation_id, billing_start, week_number, user_email]
    exec_sql(sql, params)
    return JsonResponse({'ok': True})


def reject_week(request):
    user_email = request.session['ldap_username']
    data = json.loads(request.body)
    allocation_id = int(data['allocation_id'])
    billing_start = data['billing_start']
    week_number = int(data['week_number'])
    comment = data.get('comment','')
    if not comment:
        return JsonResponse({'ok': False, 'error': 'comment required on reject'}, status=400)

    sql = """
    START TRANSACTION;
    INSERT INTO weekly_punch_confirmations
    (user_email, allocation_id, billing_start, week_number, allocated_hours, user_comment, status, actioned_by, actioned_at)
    VALUES (%s, %s, %s, %s,
      (SELECT COALESCE(wa.hours, TRUNCATE(m.total_hours / 4,2)) FROM monthly_allocation_entries m
         LEFT JOIN weekly_allocations wa ON wa.allocation_id = m.id AND wa.week_number = %s
         WHERE m.id = %s),
      %s, 'REJECTED', %s, NOW())
    ON DUPLICATE KEY UPDATE
      user_comment = VALUES(user_comment),
      status = 'REJECTED',
      actioned_by = VALUES(actioned_by),
      actioned_at = VALUES(actioned_at),
      updated_at = NOW();

    -- set TL email (lookup)
    UPDATE weekly_punch_confirmations wpc
    JOIN people_employee pe ON pe.email = %s
    SET wpc.tl_email = pe.line_manager_email
    WHERE wpc.user_email = %s AND wpc.allocation_id = %s AND wpc.billing_start = %s AND wpc.week_number = %s;

    -- insert history
    SELECT id INTO @conf_id FROM weekly_punch_confirmations
      WHERE user_email=%s AND allocation_id=%s AND billing_start=%s AND week_number=%s LIMIT 1;
    INSERT INTO weekly_punch_history (confirmation_id, actor_email, role, action, comment)
    VALUES (@conf_id, %s, 'USER', 'REJECT', %s);
    COMMIT;
    """
    params = [user_email, allocation_id, billing_start, week_number, week_number, allocation_id, comment, user_email,
              user_email, user_email, allocation_id, billing_start, week_number,
              user_email, allocation_id, billing_start, week_number, user_email, comment]
    exec_sql(sql, params)
    # notify TL (via email/in-app) - implement outside SQL
    return JsonResponse({'ok': True})
