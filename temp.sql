

CASE health_plan_type
    WHEN 'Individual health insurance issuer' THEN 'IFP'
    WHEN 'Fully insured private group health plan' THEN 'Fully Insured Group'
    WHEN 'Either partially or fully self-insured private (employment-based) group health plan' THEN 'Self-Insured / ASO'
    WHEN 'Non-federal government plan (or state or local government plan)' THEN 'State/Local Government'
    WHEN 'Federal Employees Health Benefits (FEHB) Carrier' THEN 'FEHB'
    WHEN 'Church Plan' THEN 'Church Plan'
    WHEN 'No Plan/Issuer Response' THEN 'No Response'
    ELSE 'Other'
END AS plan_type


CASE
    -- Air Ambulance (this field = vehicle capacity, not specialty)
    WHEN data_type = 'air_ambulance' THEN 'Air Ambulance'

    -- Not reported / redacted / junk
    WHEN specialty_or_capacity_level IS NULL THEN 'N/R'
    WHEN UPPER(specialty_or_capacity_level) IN ('N/R', 'NA', 'REDACTED', 'DISP-REDACTED', 'DUPLICATE', 'PROVIDER') THEN 'N/R'

    -- === Specific specialties (most specific first) ===

    -- Neuromonitoring / IOM (before Neurology/Neurosurgery)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%INTRAOP%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEUROMONIT%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEUROMONIOT%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEUROMOMIT%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%INTEROP%NEURO%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE 'IOM%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE '% IOM%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE 'IONM%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE '% IONM%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE 'CNIM%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%OON NEUROMONITOR%' THEN 'Neuromonitoring (IOM)'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEURO MONITOR%' THEN 'Neuromonitoring (IOM)'

    -- Surgical Assist (before other surgery categories)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%SURGICAL ASSIST%' THEN 'Surgical Assist'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%SURGICAL ASST%' THEN 'Surgical Assist'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%FIRST ASSIST%' THEN 'Surgical Assist'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%SURG%ASSISTANCE%' THEN 'Surgical Assist'

    -- Neonatology (before Pediatrics)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEONAT%' THEN 'Neonatology'

    -- Neurosurgery (before Neurology and General Surgery)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEUROSURG%' THEN 'Neurosurgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEUROLOGICAL SURG%' THEN 'Neurosurgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEURO SURG%' THEN 'Neurosurgery'

    -- Orthopedic Surgery (including common typos)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%ORTHO%' THEN 'Orthopedic Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%OTRHO%' THEN 'Orthopedic Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%FOOT AND ANKLE%' THEN 'Orthopedic Surgery'

    -- Plastic / Reconstructive Surgery
    WHEN UPPER(specialty_or_capacity_level) LIKE '%PLASTI%' THEN 'Plastic / Reconstructive Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%RECONSTRUCTI%' THEN 'Plastic / Reconstructive Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%HAND SURG%' THEN 'Plastic / Reconstructive Surgery'

    -- OB/GYN
    WHEN UPPER(specialty_or_capacity_level) LIKE '%OBSTET%' THEN 'OB/GYN'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%GYNEC%' THEN 'OB/GYN'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%OBGYN%' THEN 'OB/GYN'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%OB/GYN%' THEN 'OB/GYN'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%OB GYN%' THEN 'OB/GYN'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%ENDOMETRIOSIS%' THEN 'OB/GYN'

    -- Vascular Surgery (before General Surgery and Cardiology)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%VASCULAR%SURG%' THEN 'Vascular Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%VASCULAR AND ENDO%' THEN 'Vascular Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%VASCULAR%PROCEDURE%' THEN 'Vascular Surgery'
    WHEN UPPER(specialty_or_capacity_level) = 'VASCULAR' THEN 'Vascular Surgery'

    -- Cardiothoracic Surgery (before General Surgery)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%CARDIOTHORACIC%' THEN 'Cardiothoracic Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%THORACIC SURG%' THEN 'Cardiothoracic Surgery'

    -- Spine Surgery (after Ortho/Neuro catch their specific variants)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%SPIN%SURG%' THEN 'Spine Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%SPINAL%' THEN 'Spine Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE 'SPINE%' THEN 'Spine Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '% SPINE%' THEN 'Spine Surgery'

    -- General Surgery (including trauma, bariatric)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%GENERAL SURG%' THEN 'General Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%GENERAL%BARIATRIC%' THEN 'General Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%BARIATRIC%' THEN 'General Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%TRAUMA%SURG%' THEN 'General Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%TRAUMA%CRITICAL%' THEN 'General Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%LAPAROSCOPIC SURG%' THEN 'General Surgery'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%COLORECTAL%' THEN 'General Surgery'
    WHEN UPPER(specialty_or_capacity_level) = 'SURGERY' THEN 'General Surgery'
    WHEN UPPER(specialty_or_capacity_level) = 'TRAUMA' THEN 'General Surgery'

    -- Urology
    WHEN UPPER(specialty_or_capacity_level) LIKE '%UROLOG%' THEN 'Urology'

    -- Neurology (after Neuromonitoring and Neurosurgery)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEUROLOG%' THEN 'Neurology'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NEUROPHYSI%' THEN 'Neurology'
    WHEN UPPER(specialty_or_capacity_level) = 'NEURO' THEN 'Neurology'

    -- Pain Management
    WHEN UPPER(specialty_or_capacity_level) LIKE '%PAIN M%' THEN 'Pain Management'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%PAIN MEDI%' THEN 'Pain Management'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%INTERVENTIONAL PAIN%' THEN 'Pain Management'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%INTERVENTIONAL SPINE%' THEN 'Pain Management'

    -- Anesthesiology (including typos)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%ANESTHE%' THEN 'Anesthesiology'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%ANETHESIA%' THEN 'Anesthesiology'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%ANESTHSIA%' THEN 'Anesthesiology'
    WHEN UPPER(specialty_or_capacity_level) = 'ANES' THEN 'Anesthesiology'

    -- Radiology
    WHEN UPPER(specialty_or_capacity_level) LIKE '%RADIOL%' THEN 'Radiology'

    -- Hospitalist / Hospital Medicine (before Hospital/Facility catch-all)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%HOSPITALIST%' THEN 'Hospitalist'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%HOSPITAL MED%' THEN 'Hospitalist'

    -- Internal Medicine
    WHEN UPPER(specialty_or_capacity_level) LIKE '%INTERNAL MED%' THEN 'Internal Medicine'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%CRITICAL CARE%' THEN 'Internal Medicine'

    -- Cardiology (non-surgical, including electrophysiology)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%CARDIOL%' THEN 'Cardiology'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%CARDIOVASCUL%' THEN 'Cardiology'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%ELECTROPHYSI%' THEN 'Cardiology'

    -- Gastroenterology
    WHEN UPPER(specialty_or_capacity_level) LIKE '%GASTRO%' THEN 'Gastroenterology'

    -- Lab / Pathology
    WHEN UPPER(specialty_or_capacity_level) LIKE '%PATHOL%' THEN 'Lab / Pathology'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%LABORATOR%' THEN 'Lab / Pathology'
    WHEN UPPER(specialty_or_capacity_level) = 'LAB' THEN 'Lab / Pathology'

    -- Pediatrics (remaining after Neonatology)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%PEDIATRI%' THEN 'Pediatrics'

    -- Emergency Medicine (broad catch-all for ER, including typos)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%EMERG%' THEN 'Emergency Medicine'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%EMERIG%' THEN 'Emergency Medicine'
    WHEN UPPER(specialty_or_capacity_level) = 'ED' THEN 'Emergency Medicine'
    WHEN UPPER(specialty_or_capacity_level) = 'ER' THEN 'Emergency Medicine'
    WHEN UPPER(specialty_or_capacity_level) LIKE 'ER %' THEN 'Emergency Medicine'
    WHEN UPPER(specialty_or_capacity_level) LIKE '% ER' THEN 'Emergency Medicine'

    -- Hospital / Facility (catch-all for facility-level entries, including typos)
    WHEN UPPER(specialty_or_capacity_level) LIKE '%HOSPITAL%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%HOSPTIAL%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%HOSTIPAL%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%ACUTE CARE%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%ACUTE ACADEMIC%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%FACILITY%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NOT FOR PROFIT%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%NON PROFIT%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%SPECIAL HOSPITAL%' THEN 'Hospital / Facility'
    WHEN UPPER(specialty_or_capacity_level) LIKE '%TEACHING HOSPITAL%' THEN 'Hospital / Facility'

    ELSE 'Other'
END AS specialty_group

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
  CAST(`Initiating Party` AS STRING)  AS initiating_party,
  CASE 
    -- 1. UnitedHealth Group (Parent: UNH)
    -- Includes TPA (UMR), small group (AllSavers), and behavioral
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) RLIKE 'UNITED|UHC|UMR|OXFORD|SUREST|SIERRA|ALLSAVERS|ALL[ ]?SAVERS|GOLDEN[ ]?RULE|UNTIED' 
         AND UPPER(CAST(`Health Plan/Issuer Name` AS STRING) NOT LIKE '%UNIV%' -- Prevents "University" matches
    THEN 'UnitedHealth Group'

    -- 2. Elevance Health (The largest BCBS provider)
    -- Separated from other Blues because it is a distinct publicly traded competitor
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) RLIKE 'ANTHEM|ELEVANCE|WELLPOINT|AMERIGROUP' THEN 'Elevance Health (Anthem)'

    -- 3. Blue Cross Blue Shield (Regional/Independent)
    -- Catches regional Blues like Florida Blue, Highmark, CareFirst
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) RLIKE 'BLUE[ ]?CROSS|BLUE[ ]?SHIELD|BCBS|BC[ ]?BS|GUIDEWELL|HORIZON|REGENCE|PREMERA|HIGHMARK' 
    THEN 'BCBS (Regional)'

    -- 4. Aetna / CVS Health
    -- Includes Meritain (TPA) and major Joint Ventures (Banner, Innovation)
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) RLIKE 'AETNA|MERITAIN|BANNER[ ]?HEALTH|INNOVATION[ ]?HEALTH|ATENA|AENTA' THEN 'Aetna'

    -- 5. Cigna Healthcare
    -- Catches Great-West and the Cigna+Oscar venture
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) RLIKE 'CIGNA|GREAT[ ]?WEST|CINGA|CIGAN' THEN 'Cigna'

    -- 6. Centene Corporation
    -- This is often the "missing" piece in health data. Centene uses many local brand names.
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) RLIKE 'AMBETTER|WELLCARE|FIDELIS|HEALTH[ ]?NET|SUNSHINE[ ]?HEALTH|PEACH[ ]?STATE|SUPERIOR[ ]?HEALTH' 
    THEN 'Centene'

    -- 7. Kaiser Permanente
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) RLIKE 'KAISER|KASIER|PERMANENTE' THEN 'Kaiser Permanente'

    -- 8. Molina Healthcare
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) LIKE '%MOLINA%' THEN 'Molina'

    -- 9. Oscar Health
    -- Note: This is placed after Cigna to ensure Cigna+Oscar maps to Cigna
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) LIKE '%OSCAR%' THEN 'Oscar Health'

    -- 10. Humana
    WHEN UPPER(CAST(`Health Plan/Issuer Name` AS STRING) LIKE '%HUMANA%' THEN 'Humana'

    ELSE 'Other / Unclassified'
END AS payor_group,
  CASE
    -- === BILLING / RCM COMPANIES ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'saparm.com%' THEN 'SAP ARM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'halomd.com%' THEN 'HaloMD'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'fam-llc%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'fam-ll.com' THEN 'FAM LLC'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'totalcare.us' THEN 'Total Care'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'agshealth%' THEN 'AGS Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'ventra%' THEN 'Ventra Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'erevenuebilling%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'revenuebilling.com' THEN 'E Revenue Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'r1rcm%' THEN 'R1 RCM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'zotecpartner%' THEN 'Zotec Partners'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'rightmed%billing%' THEN 'Right Medical Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'aimbillingsolutions%' THEN 'AIM Billing Solutions'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'omsmedbilling%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'omemedbilling.com' THEN 'OMS Med Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'ftpbilling%' THEN 'FTP Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'simplexmed%' THEN 'SimplexMed'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'logixhealth%' THEN 'LogixHealth'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'nosur%bill%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'nosup%bill%' THEN 'No Surprise Bill'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'alldatahealt%' THEN 'AllData Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'preferredbillingaz%' THEN 'Preferred Billing AZ'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'syntechhealth%' THEN 'SynTech Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'usrcm%' THEN 'USRCM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'islandprofessionalbilling%' THEN 'Island Professional Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'sbsbilling%' THEN 'SBS Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'integrityrcm%' THEN 'Integrity RCM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'heightsrcm%' THEN 'Heights RCM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'karisbilling%' THEN 'Karis Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'elitebilling%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'eiltebillingllc.com' THEN 'Elite Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'nsabilling%' THEN 'NSA Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'expresserbilling%' THEN 'Express ER Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'agilityrcm%' THEN 'Agility RCM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'wincherbilling%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'iwncherbilling.com' THEN 'Wincher Billing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'ahsrcm%' THEN 'AHS RCM'

    -- === RADIOLOGY MANAGEMENT ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'sonoranrm%' THEN 'Sonoran RM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'mbbrm%' THEN 'MBB Radiology'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'radpmg%' THEN 'Radiology PMG'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'empireradrm%' THEN 'Empire Radiology RM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'radixhealth%' THEN 'Radix Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'iairm.com' THEN 'IAI Revenue Management'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'radalliancerm%' THEN 'Rad Alliance RM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'accessradrm%' THEN 'Access Radiology RM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'midstateradrm%' THEN 'Midstate Radiology RM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'smirm%' THEN 'SMI RM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'redrockrad%' THEN 'Red Rock Radiology'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'collaborativeimaging%' THEN 'Collaborative Imaging'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'racrm%' THEN 'RAC RM'

    -- === LAW FIRMS ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'callagy%' THEN 'Callagy Law'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'gottl%greenspan%' THEN 'Gottlieb & Greenspan'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'halkovichlaw%' THEN 'Halkovich Law'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'afslaw%' THEN 'AFS Law'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'glynnlegal%' THEN 'Glynn Legal'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'wolfepincavage%' THEN 'Wolfe Pincavage'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'beinhakerlaw%' THEN 'Beinhaker Law'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'khcfirm%' THEN 'KHC Firm'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'bracewell%' THEN 'Bracewell LLP'

    -- === PHYSICIAN STAFFING / MANAGEMENT ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'teamhealth%' THEN 'TeamHealth'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'envisionhealth%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'envsionhealth.com' THEN 'Envision Healthcare'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'scp%health%' THEN 'SCP Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'usacs%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'uscas.com' THEN 'USACS'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'soundphysicians%' THEN 'Sound Physicians'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'vituity%' THEN 'Vituity'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'apollomd%' THEN 'ApolloMD'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'usap.com' THEN 'US Anesthesia Partners'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'specialtycare%' THEN 'SpecialtyCare'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'orthomedstaffing%' THEN 'OrthoMed Staffing'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'forthesurg%' THEN 'For The Surgeons'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'provanesthesiology%' THEN 'ProVan Anesthesiology'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'pediatrix%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'pedatrix.com' THEN 'Pediatrix Medical'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'northstaranesthesia%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'northstardoc.com' THEN 'NorthStar Anesthesia'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'ascentemc%' THEN 'Ascent EMC'

    -- === MSO / CONSULTING / ADVISORY ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'mdcapitaladvi%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'mdcapitaladviors.com' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'mdcapitaladvsors%' THEN 'MD Capital Advisors'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'qmacsmso%' THEN 'QMACS MSO'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'gryphonhc.com' THEN 'Gryphon Healthcare'

    -- === HOSPITAL SYSTEMS ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'primehealthc%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'primeheathcare.com' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'primhealthcare.com' THEN 'Prime Healthcare'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'bmhcc.org' THEN 'Baptist Memorial'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'altushealthsystem%' THEN 'Altus Health System'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'hcahealthcare%' THEN 'HCA Healthcare'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'commonspirit%' THEN 'CommonSpirit Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'adventhealth%' THEN 'AdventHealth'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'wellstar%' THEN 'WellStar Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'tenethealth%' THEN 'Tenet Healthcare'

    -- === AIR AMBULANCE ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'gmr.net' THEN 'Global Medical Response'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'phiairmedical%' THEN 'PHI Air Medical'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'airmethods%' THEN 'Air Methods'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'apollomedflight%' THEN 'Apollo MedFlight'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'survivalflightinc%' THEN 'Survival Flight'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'lifeflight%' OR CAST(`Health Plan/Issuer Email Domain` AS STRING)  = 'lifeflightmaine.org' THEN 'LifeFlight'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'superiorambulance%' THEN 'Superior Ambulance'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'lifelinkiii%' THEN 'LifeLink III'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'mercyflight%' THEN 'Mercy Flights'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'careflite%' THEN 'CareFlite'

    -- === ER GROUPS ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'memorialvillageer%' THEN 'Memorial Village ER'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'americaser%' THEN 'AmericasER'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'complete.care%' THEN 'Complete Care'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'neighborshealth%' THEN 'Neighbors Health'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'victoriaemergency%' THEN 'Victoria Emergency'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'bellaireer%' THEN 'Bellaire ER'

    -- === IOM / NEUROMONITORING ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'nmaiom%' THEN 'NMA IOM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'unitedionm%' THEN 'United IOM'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'epiomneuro%' THEN 'EpiOM Neuro'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'ansmonitoring%' THEN 'ANS Monitoring'

    -- === OTHER NOTABLE ===
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'roundtmc%' THEN 'Round TMC'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'legacyhealthllc%' THEN 'Legacy Health LLC'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'anesthesiadynamics%' THEN 'Anesthesia Dynamics'
    WHEN CAST(`Health Plan/Issuer Email Domain` AS STRING)  LIKE 'summit-az%' THEN 'Summit AZ'

    ELSE 'Other'
END AS provider_domain_name_entity

FROM src_2024_q1_emergency;
