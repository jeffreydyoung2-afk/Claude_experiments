-- Databricks notebook source

-- MAGIC %md
-- MAGIC # CMS Federal IDR Data Ingestion (SQL)
-- MAGIC
-- MAGIC Reads existing per-file/sheet tables from your personal schema, unifies column
-- MAGIC names, cleans numeric formatting, and writes a single `dispute_line_items` Delta
-- MAGIC table plus enriched dashboard views.
-- MAGIC
-- MAGIC **How to use:**
-- MAGIC 1. Run the discovery cell to see your table names
-- MAGIC 2. Fill in your actual table names in the source mapping cell
-- MAGIC 3. Run the rest of the notebook top-to-bottom

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Discover Your Tables

-- COMMAND ----------

SHOW TABLES IN 10111_usm.jeffrey_y;

-- COMMAND ----------

-- Inspect columns of one table to verify header format.
-- Replace the table name below with one of yours.
-- DESCRIBE 10111_usm.jeffrey_y.`your_table_name_here`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Map Source Tables
-- MAGIC
-- MAGIC **Replace each `your_..._table_name` below with your actual table names** from
-- MAGIC the discovery output above. These temp views let the rest of the notebook stay
-- MAGIC static — you only edit this one cell.
-- MAGIC
-- MAGIC If a table doesn't exist (e.g., you skipped air ambulance for a quarter),
-- MAGIC comment out that CREATE VIEW line AND its matching SELECT block in Step 4.

-- COMMAND ----------

-- === 2024 Emergency Tables ===
CREATE OR REPLACE TEMP VIEW src_2024_q1_emergency AS SELECT * FROM 10111_usm.jeffrey_y.`your_2024_q1_emergency_table_name`;
CREATE OR REPLACE TEMP VIEW src_2024_q2_emergency AS SELECT * FROM 10111_usm.jeffrey_y.`your_2024_q2_emergency_table_name`;
CREATE OR REPLACE TEMP VIEW src_2024_q3_emergency AS SELECT * FROM 10111_usm.jeffrey_y.`your_2024_q3_emergency_table_name`;
CREATE OR REPLACE TEMP VIEW src_2024_q4_emergency AS SELECT * FROM 10111_usm.jeffrey_y.`your_2024_q4_emergency_table_name`;

-- === 2025 Emergency Tables ===
CREATE OR REPLACE TEMP VIEW src_2025_q1_emergency AS SELECT * FROM 10111_usm.jeffrey_y.`your_2025_q1_emergency_table_name`;
CREATE OR REPLACE TEMP VIEW src_2025_q2_emergency AS SELECT * FROM 10111_usm.jeffrey_y.`your_2025_q2_emergency_table_name`;


-- COMMAND ----------

-- Verify: check columns of one source to confirm header format
DESCRIBE src_2024_q3_emergency;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Create Target Table

-- COMMAND ----------

DROP TABLE IF EXISTS 10111_usm.jeffrey_y.dispute_line_items;

-- COMMAND ----------

CREATE TABLE 10111_usm.jeffrey_y.dispute_line_items (
  data_year                      STRING,
  data_quarter                   STRING,
  data_type                      STRING,
  dispute_number                 STRING,
  dli_number                     STRING,
  payment_determination_outcome  STRING,
  default_decision               STRING,
  type_of_dispute                STRING,
  provider_group_name            STRING,
  provider_name                  STRING,
  provider_email_domain          STRING,
  provider_npi                   STRING,
  practice_size_or_vehicle_type  STRING,
  health_plan_name               STRING,
  health_plan_email_domain       STRING,
  health_plan_type               STRING,
  determination_time             STRING,
  idre_compensation              DOUBLE,
  dispute_line_item_type         STRING,
  type_of_service_code           STRING,
  service_code                   STRING,
  place_of_service_code          STRING,
  item_or_service_description    STRING,
  location_of_service            STRING,
  specialty_or_capacity_level    STRING,
  provider_offer_pct_qpa         DOUBLE,
  health_plan_offer_pct_qpa      DOUBLE,
  offer_selected_from            STRING,
  prevailing_party_offer_pct_qpa DOUBLE,
  qpa_pct_median_qpa             DOUBLE,
  provider_offer_pct_median      DOUBLE,
  health_plan_offer_pct_median   DOUBLE,
  prevailing_offer_pct_median    DOUBLE,
  initiating_party               STRING
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Load Emergency Tables
-- MAGIC
-- MAGIC Column names use backtick quoting for the original CMS headers (spaces, `/`, `%`).
-- MAGIC If your tables have sanitized names (underscores), see the note at the bottom of
-- MAGIC this notebook for the alternate column names.
-- MAGIC
-- MAGIC **2024 Q1-Q2** do not have an `Initiating Party` column — those get NULL.

-- COMMAND ----------

-- === EMERGENCY: 2024 Q1 (no Initiating Party) ===
INSERT INTO 10111_usm.jeffrey_y.dispute_line_items
SELECT
  '2024' AS data_year, 'Q1' AS data_quarter, 'emergency' AS data_type,
  CAST(`Dispute Number` AS STRING)                     AS dispute_number,
  CAST(`DLI Number` AS STRING)                         AS dli_number,
  CAST(`Payment Determination Outcome` AS STRING)      AS payment_determination_outcome,
  CAST(`Default Decision` AS STRING)                   AS default_decision,
  CAST(`Type of Dispute` AS STRING)                    AS type_of_dispute,
  CAST(`Provider/Facility Group Name` AS STRING)       AS provider_group_name,
  CAST(`Provider/Facility Name` AS STRING)             AS provider_name,
  CAST(`Provider Email Domain` AS STRING)              AS provider_email_domain,
  CAST(`Provider/Facility NPI Number` AS STRING)       AS provider_npi,
  CAST(`Practice/Facility Size` AS STRING)             AS practice_size_or_vehicle_type,
  CAST(`Health Plan/Issuer Name` AS STRING)            AS health_plan_name,
  CAST(`Health Plan/Issuer Email Domain` AS STRING)    AS health_plan_email_domain,
  CAST(`Health Plan Type` AS STRING)                   AS health_plan_type,
  CAST(`Length of Time to Make Determination` AS STRING) AS determination_time,
  TRY_CAST(REGEXP_REPLACE(CAST(`IDRE Compensation` AS STRING), '[$%,]', '') AS DOUBLE) AS idre_compensation,
  CAST(`Dispute Line Item Type` AS STRING)             AS dispute_line_item_type,
  CAST(`Type of Service Code` AS STRING)               AS type_of_service_code,
  CAST(`Service Code` AS STRING)                       AS service_code,
  CAST(`Place of Service Code` AS STRING)              AS place_of_service_code,
  CAST(`Item or Service Description` AS STRING)        AS item_or_service_description,
  CAST(`Location of Service` AS STRING)                AS location_of_service,
  CAST(`Practice/Facility Specialty or Type` AS STRING) AS specialty_or_capacity_level,
  TRY_CAST(REGEXP_REPLACE(CAST(`Provider/Facility Offer as % of QPA` AS STRING), '[$%,]', '') AS DOUBLE) AS provider_offer_pct_qpa,
  TRY_CAST(REGEXP_REPLACE(CAST(`Health Plan/Issuer Offer as % of QPA` AS STRING), '[$%,]', '') AS DOUBLE) AS health_plan_offer_pct_qpa,
  CAST(`Offer Selected from Provider or Issuer` AS STRING) AS offer_selected_from,
  TRY_CAST(REGEXP_REPLACE(CAST(`Prevailing Party Offer as % of QPA` AS STRING), '[$%,]', '') AS DOUBLE) AS prevailing_party_offer_pct_qpa,
  TRY_CAST(REGEXP_REPLACE(CAST(`QPA as Percent of Median QPA` AS STRING), '[$%,]', '') AS DOUBLE) AS qpa_pct_median_qpa,
  TRY_CAST(REGEXP_REPLACE(CAST(`Provider/Facility Offer as Percent of Median Provider/Facility Offer Amount` AS STRING), '[$%,]', '') AS DOUBLE) AS provider_offer_pct_median,
  TRY_CAST(REGEXP_REPLACE(CAST(`Health Plan/Issuer Offer as Percent of Median Health Plan/Issuer Offer Amount` AS STRING), '[$%,]', '') AS DOUBLE) AS health_plan_offer_pct_median,
  TRY_CAST(REGEXP_REPLACE(CAST(`Prevailing Offer as Percent of Median Prevailing Offer Amount` AS STRING), '[$%,]', '') AS DOUBLE) AS prevailing_offer_pct_median,
  NULL AS initiating_party
FROM src_2024_q1_emergency;
