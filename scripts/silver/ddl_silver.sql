/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
IF OBJECT_ID('silver.sm_customers') IS NOT NULL DROP TABLE silver.sm_customers;
GO
CREATE TABLE silver.sm_customers (
  event_id          VARCHAR(64)   NOT NULL,
  event_name          VARCHAR(200)    NULL,
  event_type            VARCHAR(40)    NULL,
  event_date        VARCHAR(40)   NULL,
          VARCHAR(80)    NULL,
  customer_state       VARCHAR(40)    NULL,   
  zip_code             VARCHAR(16)    NULL,
  source_create_date   DATE           NULL,   
  source_referral      VARCHAR(40)    NULL,   
  opt_in               TINYINT        NULL,   
  favorite_team        VARCHAR(255)    NULL,
  dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

GO

IF OBJECT_ID('silver.sm_events') IS NOT NULL DROP TABLE silver.sm_events;
GO
CREATE TABLE silver.sm_events (
    event_id        INT,
    event_name      VARCHAR(100),
    event_type      VARCHAR(50),
    event_date      DATETIME,
    start_time      TIME,
    end_time        TIME,
    venue_name      VARCHAR(100),
    event_city      VARCHAR(100),
    venue_capacity  INT,
    home_team       VARCHAR(100),
    away_team       VARCHAR(100),
    season          VARCHAR(20),
    is_special_event BIT,
     dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.apd_demographics') IS NOT NULL DROP TABLE silver.apd_demographics;
GO
CREATE TABLE silver.apd_demographics (
  customer_id           VARCHAR(255),
  household_income_band VARCHAR(20),
  age_band              VARCHAR(10),
  gender                VARCHAR(20),
  home_ownership        VARCHAR(10),
  favorite_sport        VARCHAR(50),
  dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.trs_ticket_orders') IS NOT NULL DROP TABLE silver.trs_ticket_orders;
GO
CREATE TABLE silver.trs_ticket_orders (
  order_id          VARCHAR(255),
  order_date        DATETIME,
  customer_id       VARCHAR(255),
  email_address     VARCHAR(255),
  purchase_channel  VARCHAR(20),
  payment_method    VARCHAR(20),
  order_total       DECIMAL(10,2),
  tax               DECIMAL(10,2),
  fee               DECIMAL(10,2)
);
GO

IF OBJECT_ID('silver.trs_ticket_order_items') IS NOT NULL DROP TABLE silver.trs_ticket_order_items;
GO
CREATE TABLE silver.trs_ticket_order_items (
  order_item_id       VARCHAR(255),
  order_id            VARCHAR(64),
  event_id            VARCHAR(64),
  ticket_type         VARCHAR(50),
  section             VARCHAR(50),
  section_row         VARCHAR(50),
  seat                VARCHAR(50),
  ticket_price        DECIMAL(10,2),
  barcode             VARCHAR(100)
);
GO

IF OBJECT_ID('silver.trs_attendance') IS NOT NULL DROP TABLE silver.trs_attendance;
GO
CREATE TABLE silver.trs_attendance (
    scan_id VARCHAR(255),
    event_id VARCHAR(50),
    barcode VARCHAR(255),
    scan_time TIME,
    gate_id VARCHAR(10),
    is_valid BIT
);
GO

IF OBJECT_ID('silver.crm_campaigns') IS NOT NULL DROP TABLE silver.crm_campaigns;
GO
CREATE TABLE silver.crm_campaigns (
    campaign_id NVARCHAR(255),
    campaign_name NVARCHAR(255),
    launch_date DATE,
    campaign_purpose NVARCHAR(255),
    marketing_channel NVARCHAR(255),
    marketing_segment NVARCHAR(255),
    is_a_b BIT,
    notes NVARCHAR(MAX),
    campaign_source NVARCHAR(255),
    load_time DATETIME
);
GO 
IF OBJECT_ID('silver.crm_email_events') IS NOT NULL DROP TABLE silver.crm_email_events;
CREATE TABLE silver.crm_email_events (
    campaign_id NVARCHAR(255),
    message_id NVARCHAR(255),
    customer_email NVARCHAR(255),
    customer_id NVARCHAR(100),
    event_time_stamp DATETIME,
    event_type NVARCHAR(100),
    device NVARCHAR(255),
    link_url NVARCHAR(MAX),
    bounce_reason NVARCHAR(255)
);



