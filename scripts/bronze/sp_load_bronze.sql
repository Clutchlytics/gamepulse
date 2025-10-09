/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.sp_load_bronze;
===============================================================================
*/ 
CREATE OR ALTER PROCEDURE bronze.sp_load_bronze AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        -- Source Master Tables
        PRINT '------------------------------------------------';
        PRINT 'Loading Source Master Tables';
        PRINT '------------------------------------------------';

        -- Customers
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.sm_customers';
        TRUNCATE TABLE bronze.sm_customers;

        PRINT '>> Inserting Data Into: bronze.sm_customers';
        BULK INSERT bronze.sm_customers
        FROM 'C:\Users\tesia\OneDrive\Desktop\Cayston\GamePulse\Source Master\GamePulseTX_raw_customers.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

      
        -- Events
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.sm_events';
        TRUNCATE TABLE bronze.sm_events;

        PRINT '>> Inserting Data Into: bronze.sm_events';
        BULK INSERT bronze.sm_events
        FROM 'C:\Users\tesia\OneDrive\Desktop\Cayston\GamePulse\Source Master\GamePulseTX_raw_events.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

      
        -- Appends
        PRINT '------------------------------------------------';
        PRINT 'Loading Appends Tables';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.apd_demographics';
        TRUNCATE TABLE bronze.apd_demographics;

        PRINT '>> Inserting Data Into: bronze.apd_demographics';
        BULK INSERT bronze.apd_demographics
        FROM 'C:\Users\tesia\OneDrive\Desktop\Cayston\GamePulse\Appends\appends_raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

       
        -- Ticket Resource System
        PRINT '------------------------------------------------';
        PRINT 'Loading Ticket Resource System Tables';
        PRINT '------------------------------------------------';

        -- Orders
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.trs_ticket_orders';
        TRUNCATE TABLE bronze.trs_ticket_orders;

        PRINT '>> Inserting Data Into: bronze.trs_ticket_orders';
        BULK INSERT bronze.trs_ticket_orders
        FROM 'C:\Users\tesia\OneDrive\Desktop\Cayston\GamePulse\Ticketing Resource System\ticket_orders_raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- Order Items
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.trs_ticket_order_items';
        TRUNCATE TABLE bronze.trs_ticket_order_items;

        PRINT '>> Inserting Data Into: bronze.trs_ticket_order_items';
        BULK INSERT bronze.trs_ticket_order_items
        FROM 'C:\Users\tesia\OneDrive\Desktop\Cayston\GamePulse\Ticketing Resource System\ticket_items_raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Attendance
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.trs_attendance';
        TRUNCATE TABLE bronze.trs_attendance;

        PRINT '>> Inserting Data Into: bronze.trs_attendance';
        BULK INSERT bronze.trs_attendance
        FROM 'C:\Users\tesia\OneDrive\Desktop\Cayston\GamePulse\Ticketing Resource System\ticket_attendance_raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

     

        -- CRM
        PRINT '------------------------------------------------';
        PRINT 'Loading Customer Relationship Management Tables';
        PRINT '------------------------------------------------';

        -- Campaigns
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_campaigns';
        TRUNCATE TABLE bronze.crm_campaigns;

        PRINT '>> Inserting Data Into: bronze.crm_campaigns';
        BULK INSERT bronze.crm_campaigns
        FROM 'C:\Users\tesia\OneDrive\Desktop\Cayston\GamePulse\Customer Relationship Management\email_campaigns_raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- Email Events
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_email_events';
        TRUNCATE TABLE bronze.crm_email_events;

        PRINT '>> Inserting Data Into: bronze.crm_email_events';
        BULK INSERT bronze.crm_email_events
        FROM 'C:\Users\tesia\OneDrive\Desktop\Cayston\GamePulse\Customer Relationship Management\email_stats_raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


                SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
