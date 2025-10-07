/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.sm_customers') IS NOT NULL DROP TABLE bronze.sm_customers;
GO
CREATE TABLE bronze.sm_customers (
  cust_id        VARCHAR(64)    NULL,
  Fn             VARCHAR(80)    NULL,
  Ln             VARCHAR(80)    NULL,
  EmailAddress   VARCHAR(320)   NULL,
  Ph             VARCHAR(32)    NULL,
  Ct             VARCHAR(80)    NULL,
  St             VARCHAR(40)    NULL,   
  ZipCd          VARCHAR(16)    NULL,
  CreateDate     DATE           NULL,   
  src            VARCHAR(40)    NULL,   
  MktOptIn       TINYINT        NULL,   
  FavTeam        VARCHAR(80)    NULL,
  source_file    VARCHAR(260)   NULL,
  load_ts        DATETIME2(0)   NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


IF OBJECT_ID('bronze.apd_demographics') IS NOT NULL
  DROP TABLE bronze.apd_demographics;
GO

CREATE TABLE bronze.apd_demographics (
  cust_id     VARCHAR(64)   NOT NULL,  
  HHIncBand   VARCHAR(32)   NULL,      
  AgeBrkt     VARCHAR(32)   NULL,      
  Gdr         VARCHAR(2)    NULL,      
  HomeOwn     VARCHAR(16)   NULL,      
  FavSport    VARCHAR(40)   NULL,      
  source_file VARCHAR(260)  NULL,      
  load_ts     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


IF OBJECT_ID('bronze.trs_ticket_orders') IS NOT NULL
  DROP TABLE bronze.trs_ticket_orders;
GO

CREATE TABLE bronze.trs_ticket_orders (
  OrdId       VARCHAR(64)   NOT NULL,
  OrdDt       DATETIME2(0)  NOT NULL,
  CustRef     VARCHAR(64)   NOT NULL,       
  EmailAddr   VARCHAR(320)  NOT NULL,      
  Chanl       VARCHAR(40)   NULL,          
  PayMthd     VARCHAR(40)   NULL,           
  OrdTotal    DECIMAL(12,2) NOT NULL,
  Tx          DECIMAL(12,2) NULL,
  Fee         DECIMAL(12,2) NULL,
  source_file VARCHAR(260)  NULL,
  load_ts     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


IF OBJECT_ID('bronze.trs_ticket_order_items') IS NOT NULL
  DROP TABLE bronze.trs_ticket_order_items;
GO

CREATE TABLE bronze.trs_ticket_order_items (
  ItemId      VARCHAR(64)   NOT NULL,
  OrdId       VARCHAR(64)   NOT NULL,
  EvtId       VARCHAR(64)   NOT NULL,
  TktType     VARCHAR(40)   NULL,
  Sect        VARCHAR(40)   NULL,
  Rw          VARCHAR(16)   NULL,
  Seat        VARCHAR(16)   NULL,
  Price       DECIMAL(12,2) NULL,
  Barcode     VARCHAR(64)   NOT NULL,  -- ensured present
  source_file VARCHAR(260)  NULL,
  load_ts     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


IF OBJECT_ID('bronze.sm_events') IS NOT NULL
  DROP TABLE bronze.sm_events;
GO

CREATE TABLE bronze.sm_events (
  EvtId       VARCHAR(64)  NOT NULL,
  EvNm        VARCHAR(200) NULL,
  EvTyp       VARCHAR(40)  NULL,
  EvDt        VARCHAR(40)  NULL,     
  StTm        VARCHAR(40)  NULL,
  EndTm       VARCHAR(40)  NULL,
  VenNm       VARCHAR(120) NULL,
  City        VARCHAR(80)  NULL,
  Cap         VARCHAR(40)  NULL,
  HomeTm      VARCHAR(120) NULL,
  OppTm       VARCHAR(120) NULL,
  Season      VARCHAR(16)  NULL,
  IsSpec      VARCHAR(10)  NULL,
  source_file VARCHAR(260) NULL,
  load_ts DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


IF OBJECT_ID('bronze.trs_attendance') IS NOT NULL
  DROP TABLE bronze.trs_attendance;
GO

CREATE TABLE bronze.trs_attendance (
  ScanId      VARCHAR(64)   NOT NULL,
  Evt         VARCHAR(64)   NOT NULL,
  Bc          VARCHAR(64)   NOT NULL,
  ScanTs      DATETIME2(0)  NOT NULL,
  Gate        VARCHAR(20)   NULL,
  Valid       TINYINT       NOT NULL,     
  source_file VARCHAR(260)  NULL,
  load_ts     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('bronze.crm_campaigns') IS NOT NULL
  DROP TABLE bronze.crm_campaigns;
GO

CREATE TABLE bronze.crm_campaigns (
  CampId      VARCHAR(32)  NOT NULL,
  CampNm      VARCHAR(200) NULL,
  LaunchDt    VARCHAR(40)  NULL,   
  Purp        VARCHAR(40)  NULL,  
  Chnl        VARCHAR(40)  NULL,  
  Seg         VARCHAR(120) NULL,  
  Bnr         VARCHAR(120) NULL,  
  IsA_B       VARCHAR(10)  NULL,  
  Notes       VARCHAR(255) NULL,
  Src         VARCHAR(260) NULL,
  LoadTs      VARCHAR(32)  NULL,   -- keep as raw string for lineage parity
  source_file VARCHAR(260) NULL,
  load_ts     DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('bronze.crm_email_events') IS NOT NULL
  DROP TABLE bronze.crm_email_events;
GO

CREATE TABLE bronze.crm_email_events (
  CampId      VARCHAR(32)  NOT NULL,
  MsgId       VARCHAR(64)  NULL,
  Email       VARCHAR(320) NOT NULL,
  CustRef     VARCHAR(64)  NULL,
  EvtTyp      VARCHAR(20)  NOT NULL,  -- send|delivered|open|click|bounce|unsubscribe
  EvtTs       VARCHAR(40)  NOT NULL,  -- messy timestamp string
  UserAgent   VARCHAR(400) NULL,
  LinkUrl     VARCHAR(500) NULL,
  BounceCd    VARCHAR(64)  NULL,
  source_file VARCHAR(260) NULL,
  load_ts     DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);


