-- ================================================
-- Data Consistency Note
-- ================================================
-- Since this dataset was generated using synthetic data tools like Faker,
-- certain inconsistencies exist compared to traditional ETL pipelines.
-- These updates are applied to align the data with realistic business logic,
-- ensuring referential integrity, consistent identifiers, and plausible event flows.
-- This step helps simulate production-like behavior for testing, analytics, and modeling.
-- ================================================
USE GamePulse;

UPDATE silver.sm_customers
SET email_address = LEFT(email_address, CHARINDEX('@', email_address) - 1) + '@gampulse.com';

-- Create a temporary mapping between crm_email_events and sm_customers
-- Step 1: Create a temporary mapping table between crm_email_events and sm_customers
WITH DistinctEmails AS (
    SELECT DISTINCT customer_email
    FROM silver.crm_email_events
),
EmailEvents AS (
    SELECT customer_email,
           ROW_NUMBER() OVER (ORDER BY NEWID()) AS rn
    FROM DistinctEmails
),
CustomerEmails AS (
    SELECT email_address,
           ROW_NUMBER() OVER (ORDER BY NEWID()) AS rn
    FROM silver.sm_customers
)
SELECT 
    e.customer_email AS old_email,
    c.email_address AS new_email
INTO #EmailMapping
FROM EmailEvents e
JOIN CustomerEmails c ON e.rn = c.rn;

SELECT * FROM #EmailMapping

UPDATE e
SET e.customer_email = m.new_email
FROM silver.crm_email_events e
JOIN #EmailMapping m ON e.customer_email = m.old_email;

UPDATE e
SET e.customer_id = c.customer_id
FROM silver.crm_email_events e
JOIN silver.sm_customers c ON e.customer_email = c.email_address
WHERE e.customer_id IS NULL;

SELECT * FROM silver.crm_email_events
