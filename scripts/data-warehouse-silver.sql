--INITIAL CHECK & DATA EXPLORATION
SELECT TOP (0) * FROM [DataWarehouse].[bronze].[crm_cust_info];
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[crm_prd_info];
SELECT TOP (0) * FROM [DataWarehouse].[bronze].[crm_sales_details];
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[erp_cust_az12];
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[erp_loc_a101];
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]

--CREATE TABLE
IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status	NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id				INT,
	cat_id				NVARCHAR(50),
	cat_id_2			NVARCHAR(50),
	prd_key				NVARCHAR(50),
	prd_nm				NVARCHAR(50),
	prd_cost			INT,
	prd_line			NVARCHAR(50),
	prd_start_dt		DATE,
	prd_end_dt			DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num			NVARCHAR(50),
	sls_prd_key			NVARCHAR(50),
	sls_cust_id			NVARCHAR(50),
	sls_order_dt		DATE,
	sls_ship_dt			DATE,
	sls_due_dt			DATE,
	sls_sales			INT,
	sls_quantity		INT,
	sls_price			INT,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
	cid					NVARCHAR(50),
	bdate				DATE,
	gen					NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid					NVARCHAR(50),
	cntry				NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
	id					NVARCHAR(50),
	cat					NVARCHAR(50),
	subcat				NVARCHAR(50),
	maintenance			NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
)

--=========================================
--DATA CLEAN!
--=========================================

-----------------------------------------------------------------------
--check for nulls or duplicates in primary key (expectation: no result)
-----------------------------------------------------------------------

--check duplicates
SELECT cst_id, COUNT(*) AS count_num
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--check for 29466
SELECT * FROM bronze.crm_cust_info
WHERE cst_id = 29466

--query for non duplicate
SELECT *
FROM (
	SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t
WHERE flag_last = 1

-----------------------------------------------------------------------
--check unwanted spaces (expectation: no results)
-----------------------------------------------------------------------

--check for firstname
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--check for lastname
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

--using the non duplicate query
SELECT TOP(1000)
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,	
cst_marital_status,	
cst_gndr,	
cst_create_date
FROM (
	SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t
WHERE flag_last = 1

-----------------------------------------------------------------------
--data standardization & consistency
-----------------------------------------------------------------------

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

--combining with previous query
SELECT TOP(1000)
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,	
CASE	WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
		WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
		ELSE 'n/a'
END cst_marital_status,	
CASE	WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
		WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
		ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t
WHERE flag_last = 1

--=========================================
--INSERT
--=========================================

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,	
	cst_marital_status,	
	cst_gndr,
	cst_create_date)

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,	
CASE	WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
		WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
		ELSE 'n/a'
END cst_marital_status,	
CASE	WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
		WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
		ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t
WHERE flag_last = 1

--================================================================
--=============================AGAIN==============================
--================================================================

select top(1000) * from bronze.crm_prd_info

SELECT prd_id, COUNT(*) AS count_num
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key)

SELECT
prd_id,
prd_key,	
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS cat_id_2,
prd_nm,	
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))	
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,	
CAST(LEAD(prd_start_dt) OVER (PARTITION BY  prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
FROM bronze.crm_prd_info

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

------------------------------------------------------------
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	cat_id_2,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)

SELECT
prd_id,
prd_key,	
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS cat_id_2,
prd_nm,	
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))	
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,	
CAST(LEAD(prd_start_dt) OVER (PARTITION BY  prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
FROM bronze.crm_prd_info

---------------------------------------------------------

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price 
FROM bronze.crm_sales_details
--WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
--WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

--check for invalids date
SELECT 
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price 
FROM bronze.crm_sales_details

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price 
FROM bronze.crm_sales_details
WHERE 
sls_sales != sls_price * sls_quantity 
OR sls_sales <= 0 OR sls_price <= 0 OR sls_quantity <= 0
OR sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL
ORDER BY sls_sales, sls_quantity, sls_price

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
--sls_price AS old_sls_price,
CASE	WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
END AS sls_price,
sls_quantity,
--sls_sales AS old_sls_sales,
CASE	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity 
		THEN ABS(sls_price) * sls_quantity
		ELSE sls_sales
END AS sls_sales
FROM bronze.crm_sales_details

------------------------------------------------------------

INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_quantity,
	sls_price,
	sls_sales
)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
--sls_price AS old_sls_price,
CASE	WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
END AS sls_price,
sls_quantity,
--sls_sales AS old_sls_sales,
CASE	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity 
		THEN ABS(sls_price) * sls_quantity
		ELSE sls_sales
END AS sls_sales
FROM bronze.crm_sales_details

--------------------------------------------------------------

SELECT  
CASE	WHEN cid LIKE 'NAS%' 
		THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END AS cid,
CASE	WHEN bdate > GETDATE()
		THEN NULL
		ELSE bdate
END AS bdate,
CASE	WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
		ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;
--WHERE CASE	WHEN cid LIKE 'NAS%' 
--			THEN SUBSTRING(cid, 4, LEN(cid))
--			ELSE cid
--END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

SELECT bdate FROM  bronze.erp_cust_az12
WHERE bdate > GETDATE()

INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)

SELECT  
CASE	WHEN cid LIKE 'NAS%' 
		THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END AS cid,
CASE	WHEN bdate > GETDATE()
		THEN NULL
		ELSE bdate
END AS bdate,
CASE	WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
		ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-----------------------------------------------

SELECT 
REPLACE(cid, '-', '') cid,
CASE	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
		ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

SELECT DISTINCT
CASE	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
		ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)
SELECT 
REPLACE(cid, '-', '') cid,
CASE	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
		ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

-------------------------------------------------------------

SELECT TOP (1000) * FROM [DataWarehouse].[silver].[crm_cust_info];
SELECT TOP (1000) * FROM [DataWarehouse].[silver].[crm_prd_info];
SELECT  * FROM [DataWarehouse].[silver].[crm_sales_details];
SELECT * FROM [DataWarehouse].[silver].[erp_cust_az12];
SELECT TOP (1000) * FROM [DataWarehouse].[silver].[erp_loc_a101];
SELECT TOP (1000) * FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]

SELECT id,COUNT(*) AS count_num
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)

SELECT * FROM bronze.erp_px_cat_g1v2

--==================================================================
--CREATE EXEC WITH TRUNCATION (just like the bronze layer)
--==================================================================
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY	
	PRINT '======================================='
	PRINT 'LOADING SILVER LAYER'
	PRINT '======================================='

	PRINT '---------------------------------------'
	PRINT 'LOADING CRM TABLE'
	PRINT '---------------------------------------'
	
--1.--------------------------------
SET @batch_start_time = GETDATE();
	SET @start_time = GETDATE();
	PRINT '>> Truncating table: silver.crm_cust_info'
	TRUNCATE TABLE silver.crm_cust_info
	PRINT '>> Inserting data into table: silver.crm_cust_info'
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,	
	cst_marital_status,	
	cst_gndr,
	cst_create_date)

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,	
CASE	WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
		WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
		ELSE 'n/a'
END cst_marital_status,	
CASE	WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
		WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
		ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t
WHERE flag_last = 1

SET @end_time = GETDATE();
	PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT ' '
--2.--------------------------------
SET @batch_start_time = GETDATE();
	SET @start_time = GETDATE();
	PRINT '>> Truncating table: silver.crm_prd_info'
	TRUNCATE TABLE silver.crm_prd_info
	PRINT '>> Inserting data into table: silver.crm_prd_info'
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	cat_id_2,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)

SELECT
prd_id,
prd_key,	
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS cat_id_2,
prd_nm,	
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))	
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,	
CAST(LEAD(prd_start_dt) OVER (PARTITION BY  prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
FROM bronze.crm_prd_info
SET @end_time = GETDATE();
	PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT ' '
--3.--------------------------------
SET @batch_start_time = GETDATE();
	SET @start_time = GETDATE();
	PRINT '>> Truncating table: silver.crm_sales_details'
	TRUNCATE TABLE silver.crm_sales_details
	PRINT '>> Inserting data into table: silver.crm_sales_details'
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_quantity,
	sls_price,
	sls_sales
)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
--sls_price AS old_sls_price,
CASE	WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
END AS sls_price,
sls_quantity,
--sls_sales AS old_sls_sales,
CASE	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity 
		THEN ABS(sls_price) * sls_quantity
		ELSE sls_sales
END AS sls_sales
FROM bronze.crm_sales_details
	SET @end_time = GETDATE();
	PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT ' '
	PRINT '---------------------------------------'
	PRINT 'LOADING ERP TABLE'
	PRINT '---------------------------------------'
--4.--------------------------------
SET @batch_start_time = GETDATE();
	SET @start_time = GETDATE();
	PRINT '>> Truncating table: silver.erp_cust_az12'
	TRUNCATE TABLE silver.erp_cust_az12
	PRINT '>> Inserting data into table: silver.erp_cust_az12'
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)

SELECT  
CASE	WHEN cid LIKE 'NAS%' 
		THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
END AS cid,
CASE	WHEN bdate > GETDATE()
		THEN NULL
		ELSE bdate
END AS bdate,
CASE	WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
		ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;
SET @end_time = GETDATE();
	PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT ' '
--5.--------------------------------
SET @batch_start_time = GETDATE();
	SET @start_time = GETDATE();
	PRINT '>> Truncating table: silver.erp_loc_a101'
	TRUNCATE TABLE silver.erp_loc_a101
	PRINT '>> Inserting data into table: silver.erp_loc_a101'
INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)
SELECT 
REPLACE(cid, '-', '') cid,
CASE	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
		ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;
SET @end_time = GETDATE();
	PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT ' '
--6.--------------------------------
SET @batch_start_time = GETDATE();
	SET @start_time = GETDATE();
	PRINT '>> Truncating table: silver.erp_px_cat_g1v2'
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT '>> Inserting data into table: silver.erp_px_cat_g1v2'
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)

SELECT * FROM bronze.erp_px_cat_g1v2
SET @end_time = GETDATE();
	PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT ' '
	SET @batch_end_time = GETDATE();
	PRINT '>> Load batch duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
	PRINT ' ';
	END TRY
	BEGIN CATCH
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR)
		PRINT '========================================='
	END CATCH
END

EXEC silver.load_silver