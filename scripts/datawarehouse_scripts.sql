USE master;

-- Creating database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Creating schema for all 3 layers
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

------------------------------------- Bronze Layer ----------------------------------------------------
--  Creating Tables - DDL

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
    DROP TABLE  bronze.crm_cust_info

CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
Go

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
    DROP TABLE  bronze.crm_prd_info

CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);
GO

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
    DROP TABLE  bronze.crm_sales_details

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
    DROP TABLE  bronze.erp_loc_a101

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
    DROP TABLE  bronze.erp_cust_az12

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE  bronze.erp_px_cat_g1v2

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

--- Loading Data into tables - Bulk Insert --- Creating Stored Procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==============================================';
        PRINT 'Loading Bronze Layer';
        PRINT '==============================================';

        PRINT '----------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info; --- Table 1

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\DAT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';

        TRUNCATE TABLE bronze.crm_prd_info; --- Table 2

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\DAT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );

        TRUNCATE TABLE bronze.crm_sales_details; --- Table 3

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\DAT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
             );

        PRINT '----------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '----------------------------------------------';

        TRUNCATE TABLE bronze.erp_loc_a101; --- Table 4

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\DAT\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
             );

        TRUNCATE TABLE bronze.erp_cust_az12; --- Table 5

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\DAT\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
             );

        TRUNCATE TABLE bronze.erp_px_cat_g1v2; --- Table 6

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\DAT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
             );
        SET @batch_end_time = GETDATE();
        PRINT '==============================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT ' - Total Load Duration: ' +  CAST(DATEDIFF(second,@batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURED DURING BRONZE LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
    END CATCH
END

------------------------------------- Silver Layer ----------------------------------------------------
--  Creating Tables - DDL

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE  silver.crm_cust_info

CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE  silver.crm_prd_info

CREATE TABLE silver.crm_prd_info(
	prd_id INT,
    cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE  silver.crm_sales_details

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
    DROP TABLE  silver.erp_loc_a101

CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
    DROP TABLE  silver.erp_cust_az12

CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE  silver.erp_px_cat_g1v2

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-------------- Quality checks and transformations -----------------------------------------------------------------

-----------------------------Table 1 ( Customer Information) ---------------------------------------------

----- checking duplicates in primary key(cst_id) ----------------
SELECT cst_id,COUNT(*) 
FROM bronze.crm_cust_info
Group BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL;

-- flaging the latest entry
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
where cst_id = 29466;

-- now will get proper cst_id - by removing duplicates and null values
SELECT * FROM
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL
) t --alias
WHERE flag_last =1;

---- Checking unwanted spaces (cst_firstname,cst_lastname,cst_material_status,cst_gndr)---------------
SELECT * 
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR 
cst_lastname != TRIM(cst_lastname) OR
cst_material_status != TRIM(cst_material_status) OR
cst_gndr != TRIM(cst_gndr);

-- Trim columns and transformation
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
TRIM(cst_material_status) AS cst_material_status,
TRIM(cst_gndr) AS cst_gndr,
cst_create_date
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL
) t --alias
WHERE flag_last =1;

---- Checking the consistency of values (cst_material_status,cst_gndr) ---------------

-- Data Standardization & Consistency & inserting the data into silver.customer table ----- inserting table 1 
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==============================================';
        PRINT 'Loading Silver Layer';
        PRINT '==============================================';

    TRUNCATE TABLE silver.crm_cust_info
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date)

    SELECT 
        cst_id,
        cst_key,

        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,

        CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
             WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
             ELSE 'Unknown'
        END cst_material_status,

        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
             WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
             ELSE 'Unknown'
        END cst_gndr,

        cst_create_date

    FROM (
        SELECT *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL
        ) t --alias
    WHERE flag_last =1;

    -------------------- Table 2 ( Product Information) -------------------------
    TRUNCATE TABLE silver.crm_prd_info

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

            REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,-- this helps to connect with table (bronze.erp_cust_az12)

            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,-- this helps to connect with table (bronze.crm_sales_details) --- Derived Columns

            TRIM(prd_nm) AS prd_nm,

            ISNULL(prd_cost,0) AS prd_cost, -- cost can't be null or negative -- handling missing values
            /*
            CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                 ELSE 'Unknown'
            END prd_line, -- below alternative way is written for same transformation
            */
            CASE UPPER(TRIM(prd_line))
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'R' THEN 'Road'
                 WHEN 'T' THEN 'Touring'
                 WHEN 'S' THEN 'Other Sales'
                 ELSE 'Unknown'
            END prd_line,

            CAST (prd_start_dt As DATE) AS prd_start_dt, -- should be greater than end date - chnaging the data type 
            CAST (DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))AS DATE) AS prd_end_dt --- data enrichment (getting proper end date using lead function which gives the next value)
        FROM bronze.crm_prd_info;


    /*
    SELECT
        prd_id,
        prd_key,
        prd_nm,
        prd_start_dt,
        prd_end_dt,
        DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt_test --- getting proper end date usin lead function which gives the next value
    FROM bronze.crm_prd_info
    WHERE prd_key IN ( 'AC-HE-HL-U509-R','AC-HE-HL-U509') */

    -------------------- Table 3 ( Sales Information) ---------------------------------------
    TRUNCATE TABLE silver.crm_sales_details
    INSERT INTO silver.crm_sales_details ( 
        sls_ord_num,  
        sls_prd_key, 
        sls_cust_id,  
        sls_order_dt, 
        sls_ship_dt,  
        sls_due_dt,   
        sls_sales,   
        sls_quantity, 
        sls_price)
    SELECT 
    TRIM(sls_ord_num) AS sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- we only need proper date
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) ---- SQL we cannot convert int to date directly , we have to convert it into varchar first
    END AS sls_order_dt,

    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL -- we only need proper date
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) ---- SQL we cannot convert int to date directly , we have to convert it into varchar first
    END AS sls_ship_dt,

    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL -- we only need proper date
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) ---- SQL we cannot convert int to date directly , we have to convert it into varchar first
    END AS sls_due_dt,

    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS (sls_price)
         ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity ,0) --- nullif for future case - if there is 0 in quantity column
         ELSE sls_price
    END AS sls_price

    FROM bronze.crm_sales_details;

    /*
    SELECT * FROM bronze.crm_sales_details
    where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt; -- order date should always smaller than shipping date and due date

    SELECT 
    NULLIF(sls_order_dt,0) sls_order_dt from bronze.crm_sales_details
    where sls_order_dt <=0 or 
    len(sls_order_dt) != 8 or 
    sls_order_dt > 20500101 or  
    sls_order_dt < 19000101; --- data validation for date column

    SELECT * FROM bronze.crm_sales_details
    where sls_sales != sls_quantity *sls_price OR
    sls_sales is null OR sls_sales <= 0 OR
    sls_price is null or sls_price <= 0  or
    sls_quantity is null OR sls_quantity <= 0 --- rules to clean -- if sales is negative or null derive it using qantity and price vice versa and convert th negative to positive directly */

    -------------------- Table 4 (erp - Customer Birthday) ---------------------------------------
    TRUNCATE TABLE silver.erp_cust_az12
    INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)

    SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
             ELSE cid
        END cid,

        CASE WHEN bdate >GETDATE() THEN NULL
             ELSE bdate
        END bdate,
   
        CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female' 
             WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
             ELSE 'Unknown'
        END AS gen

    FROM bronze.erp_cust_az12;

    /* 
    SELECT 
    bdate
    FROM bronze.erp_cust_az12
    where bdate < '1924-01-01' OR bdate > GETDATE();

    SELECT distinct
    gen
    FROM bronze.erp_cust_az12; */

    -------------------- Table 5 (erp - Customer Location) ---------------------------------------
    TRUNCATE TABLE silver.erp_loc_a101
    INSERT INTO silver.erp_loc_a101 (cid,cntry)
    SELECT 
    REPLACE(cid,'-','') cid, -- for connecting the tables using key we need identical keys
    CASE WHEN UPPer(TRIM(cntry)) = 'DE' THEN 'Germany' -- data validation and consistency
         WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
         WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'Unknown'
         ELSE TRIM(cntry)
    END cntry
    FROM bronze.erp_loc_a101;

    /*SELECT distinct
    cntry
    FROM bronze.erp_loc_a101; */

    -------------------- Table 6 (erp - Customer Location) ---------------------------------------
    TRUNCATE TABLE silver.erp_px_cat_g1v2
    INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
    SELECT 
        TRIM(id),
        TRIM(cat),
        TRIM(subcat),
        TRIM(maintenance)
    FROM bronze.erp_px_cat_g1v2 -- this table is clean and no transforamtion is required.

    SET @batch_end_time = GETDATE();
        PRINT '==============================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT ' - Total Load Duration: ' +  CAST(DATEDIFF(second,@batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURED DURING SILVER LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
    END CATCH
END


     -----------------------------------------------------------------------------------------------


--------------------------------- GOLD Layer ------------------------------------------------------
-- preparing table with user friendly names and data integration from many to one table and order of the columns

---- Dimension Table 1 - Customers ----------------------------------------------------

CREATE VIEW gold.dim_customers AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name ,
        la.cntry As country,
        ci.cst_material_status AS marital_status,
        CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM table is the source and master table 
             ELSE COALESCE(ca.gen,'Unknown')
        END AS gender,
        ca.bdate AS birthdate,
        ci.cst_create_date AS create_date

    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

/*SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
         ELSE COALESCE(ca.gen,'Unknown')
    END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1,2*/


---- Dimension Table 2 - Product ----------------------------------------------------

CREATE VIEW gold.dim_products AS

SELECT
    ROW_NUMBER() OVER ( ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost As product_cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL; --- filter out all historical dat keeping only current data

---- FACT Table - Sales ----------------------------------------------------
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number, 
    pr.product_key, --- dimension keys
    cu.customer_key,--- dimension keys  
    sd.sls_order_dt AS order_date, 
    sd.sls_ship_dt AS shipping_date,  
    sd.sls_due_dt AS due_date,   
    sd.sls_sales AS sales_amount,   
    sd.sls_quantity AS quantity, 
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr 
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;

----------------------------------------------------------------------------------------
