-- ==========================================================
-- Test Data Setup
-- Creates minimal dummy data for local unit tests
-- ==========================================================

-- Ensure schemas exist
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS harmonize;
CREATE SCHEMA IF NOT EXISTS refined;

-- -------------------------
-- RAW LAYER
-- -------------------------
CREATE OR REPLACE TABLE raw.policies (
  PolicyID INT,
  CustID INT,
  PolicyDate DATE,
  PremAmt DOUBLE,
  Status STRING
);

INSERT INTO raw.policies VALUES
  (1, 100, DATE('2023-01-01'), 500.0, 'Active'),
  (2, 101, DATE('2023-02-15'), 600.0, 'Expired'),
  (3, 102, DATE('2023-03-10'), 700.0, 'Active');

CREATE OR REPLACE TABLE raw.customers (
  CustomerID INT,
  CustomerName STRING,
  Segment STRING
);

INSERT INTO raw.customers VALUES
  (100, 'Alice', 'Retail'),
  (101, 'Bob', 'Corporate'),
  (102, 'Charlie', 'SMB');

-- -------------------------
-- HARMONIZE LAYER
-- -------------------------
CREATE OR REPLACE TABLE harmonize.policies_standardized AS
SELECT
  PolicyID AS policy_id,
  CustID AS customer_id,
  PolicyDate AS policy_date,
  PremAmt AS premium_amount,
  Status AS status
FROM raw.policies;

CREATE OR REPLACE TABLE harmonize.customers_cleaned AS
SELECT
  CustomerID AS customer_id,
  CustomerName AS customer_name,
  Segment AS customer_segment
FROM raw.customers;

-- -------------------------
-- REFINED LAYER
-- -------------------------
CREATE OR REPLACE TABLE refined.policy_kpis AS
SELECT
  year(policy_date) AS policy_year,
  COUNT(*) AS total_policies,
  AVG(premium_amount) AS avg_premium,
  SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_policies
FROM harmonize.policies_standardized
GROUP BY year(policy_date);

CREATE OR REPLACE TABLE refined.customer360 AS
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_segment,
  COUNT(p.policy_id) AS total_policies,
  SUM(p.premium_amount) AS total_premium
FROM harmonize.customers_cleaned c
LEFT JOIN harmonize.policies_standardized p
  ON c.customer_id = p.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_segment;
