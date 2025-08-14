CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start_time DATETIME, @bronze_end_time DATETIME;
	SET @bronze_start_time = GETDATE();
	BEGIN TRY
		PRINT '===============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Date Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'd:\IT\SQL\SQL_Data_Warehouse_Project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT '------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Date Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'd:\IT\SQL\SQL_Data_Warehouse_Project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT '------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Date Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'd:\IT\SQL\SQL_Data_Warehouse_Project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT '------------------'

		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Date Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'd:\IT\SQL\SQL_Data_Warehouse_Project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT '------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Date Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'd:\IT\SQL\SQL_Data_Warehouse_Project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT '------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Date Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'd:\IT\SQL\SQL_Data_Warehouse_Project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
	PRINT '=============================================';
	SET @bronze_end_time = GETDATE();
	PRINT '>> Bronze Layer Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
	PRINT '=============================================';
	END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
		PRINT '===============================================';
	END CATCH;
END
