/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY c.customer_id) AS gamepulse_record_key,
c.customer_id,
d.household_income_band,
d.age_band,
d.gender,
d.home_ownership,
CASE
	WHEN c.favorite_team = 'GamePulse Texas' THEN 'football'
	ELSE d.favorite_sport
END AS favorite_sport,
c.favorite_team,
c.customer_city,
c.customer_state,
c.zip_code,
c.opt_in
FROM silver.sm_customers c
LEFT JOIN silver.apd_demographics d 
ON c.customer_id = d.customer_id
GO

-- =============================================================================
-- Create Dimension: gold.dim_events
-- =============================================================================
IF OBJECT_ID('gold.dim_events', 'V') IS NOT NULL
    DROP VIEW gold.dim_events;
GO

CREATE VIEW gold.dim_events AS
SELECT
event_id,
event_name,
event_type,
event_date,
start_time,
CASE 
	WHEN end_time IS NULL THEN DATEADD(HOUR, 3, start_time)
    ELSE end_time
END AS end_time,
venue_name,
CASE
	WHEN venue_name = 'GamePulse Arena' THEN 'Dallas' 
	ELSE event_city
END AS event_city, 
CASE
	WHEN venue_name = 'GamePulse Arena' THEN 9500
	ELSE venue_capacity
END AS venue_capacity,
home_team,
away_team,
season,
is_special_event
FROM silver.sm_events
GO

-- =============================================================================
-- Create Fact Table: gold.fct_ticketing
-- =============================================================================
IF OBJECT_ID('gold.fct_ticketing', 'V') IS NOT NULL
    DROP VIEW gold.fct_ticketing;
GO

CREATE VIEW gold.fct_ticketing AS
SELECT 
t.customer_id,
t.order_id,
t.order_date,
i.ticket_type,
i.section,
i.section_row,
i.seat,
i.ticket_price,
i.barcode,
i.event_id,
t.purchase_channel,
i.order_item_id
FROM silver.trs_ticket_orders t
LEFT JOIN silver.trs_ticket_order_items i 
	ON t.order_id = i.order_id;
GO

-- =============================================================================
-- Create Fact Table: gold.fct_attendance
-- =============================================================================
IF OBJECT_ID('gold.fct_attendance', 'V') IS NOT NULL
    DROP VIEW gold.fct_attendance;
GO

CREATE VIEW gold.fct_attendance AS
SELECT 
a.scan_id,
a.barcode,
a.event_id,
e.event_name,
de.event_date,
de.start_time AS event_start_time,
de.end_time AS event_end_time,
de.season,
a.scan_time,
a.gate_id,
a.is_valid
FROM silver.trs_attendance a
LEFT JOIN silver.sm_events e
	ON a.event_id = e.event_id
LEFT JOIN gold.dim_events de
	ON a.event_id = de.event_id
GO

-- =============================================================================
-- Create Fact Table: gold.fct_email_stats
-- =============================================================================
IF OBJECT_ID('gold.fct_email_stats', 'V') IS NOT NULL
    DROP VIEW gold.fct_email_stats;
GO

CREATE VIEW gold.email_stats AS
SELECT
    e.customer_id,
    COUNT(*) AS total_events,
    SUM(CASE WHEN e.event_type = 'send' THEN 1 ELSE 0 END) AS sends,
    SUM(CASE WHEN e.event_type = 'delivered' THEN 1 ELSE 0 END) AS deliveries,
    SUM(CASE WHEN e.event_type = 'bounce' THEN 1 ELSE 0 END) AS bounces,
    SUM(CASE WHEN e.event_type = 'unsubscribe' THEN 1 ELSE 0 END) AS unsubscribes,
    SUM(CASE WHEN e.event_type = 'open' THEN 1 ELSE 0 END) AS opens,
    SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
    MIN(e.event_time_stamp) AS first_event_time,
    MAX(e.event_time_stamp) AS last_event_time
FROM silver.crm_email_events e
WHERE e.customer_id IS NOT NULL
GROUP BY e.customer_id;
GO
