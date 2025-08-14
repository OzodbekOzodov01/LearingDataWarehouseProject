/* 
	In order to execure this prodcedure use this command:
	EXEC silver.load_silver
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @silver_start_time DATETIME, @silver_end_time DATETIME;
	SET @silver_start_time =GETDATE();
	BEGIN TRY
		PRINT '==============================================';
		PRINT '>> Loading Silver Layer';
		PRINT '==============================================';

		PRINT '==============================================';
		PRINT '>> Loading CRM Tables';
		PRINT '==============================================';
		-- ==============================================
		-- Inserting to table: silver.crm_cust_info'
		-- ==============================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info ';
		TRUNCATE TABLE DataWarehouse.silver.crm_cust_info;
		PRINT '>> Inserting Table: silver.crm_cust_info';
		INSERT INTO DataWarehouse.silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gender,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_material_status,
			CASE
				WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gender,
			cst_create_date
		FROM(
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT  '------------------'

		-- ==============================================
		-- Inserting to table: silver.crm_prd_info
		-- ==============================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info ';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Table: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			 prd_id,
			 cat_id,
			 prd_key,
			 prd_nm,
			 prd_cost,
			 prd_line,
			 prd_start_dt,
			 prd_end_dt
		) 
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			prd_start_dt,
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT  '------------------';


		-- ==============================================
		-- Inserting to table: silver.crm_sales_details
		-- ==============================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Table: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			 sls_ord_num,
			 sls_prd_key,
			 sls_cst_id,
			 sls_order_dt,
			 sls_ship_dt,
			 sls_due_dt,
			 sls_sales,
			 sls_quantity,
			 sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cst_id,
			CASE WHEN sls_order_dt =  0 OR LEN(sls_order_dt) !=8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE WHEN sls_ship_dt =  0 OR LEN(sls_ship_dt) !=8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
			CASE WHEN sls_due_dt =  0 OR LEN(sls_due_dt) !=8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales != sls_quantity * ABS(sls_price) OR  sls_sales IS NULL OR  sls_sales <= 0
				 THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END sls_sales_dt,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR  sls_price  <= 0
				 THEN sls_sales /  NULLIF(sls_quantity, 0)
				 ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT  '------------------';

		PRINT '=============================================='
		PRINT '>> Loading ERP Tables'
		PRINT '==============================================';
		-- ==============================================
		-- Inserting to table: silver.erp_cust_az12
		-- ==============================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Table: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END,
			CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IS NULL THEN 'n/a'
				WHEN UPPER(TRIM(gen)) IN( 'F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN( 'M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) = ' ' THEN 'n/a'
				ELSE gen
			END gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT  '------------------';

		-- ==============================================
		-- Inserting to table: silver.erp_loc_a101
		-- ==============================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Table: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid,
			CASE 
				WHEN TRIM(cntry) IS NULL THEN 'n/a'
				WHEN TRIM(cntry) = '' THEN 'n/a'
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) = 'US' THEN 'United States'
				WHEN TRIM(cntry) = 'USA' THEN 'United States'
				ELSE TRIM(cntry)
			END cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT  '------------------';

		-- ==============================================
		-- Inserting to table: silver.erp_px_cat_g1v2
		-- ==============================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Table: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +  'seconds';
		PRINT  '------------------';
	END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
		PRINT '===============================================';
	END CATCH
	SET @silver_end_time = GETDATE();
	PRINT '>> Load Duration Of Silver Layer:' + CAST(DATEDIFF(SECOND, @silver_start_time, @silver_end_time) AS NVARCHAR) +  'seconds';
END



