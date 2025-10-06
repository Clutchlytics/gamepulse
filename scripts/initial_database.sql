/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'GamePulse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


USE master;
GO

-- Drop and recreate the 'GamePulse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'GamePulse')
BEGIN
    ALTER DATABASE GamePulse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE GamePulse;
END;
GO

-- Create the 'GamePulse' database
CREATE DATABASE GamePulse;
GO

USE GamePulse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
