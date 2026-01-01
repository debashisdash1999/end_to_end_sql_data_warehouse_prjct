/* ============================================================
   Database Context
   ------------------------------------------------------------
   Sets the active database for Silver layer transformations.
   ============================================================ */
USE Datawarehouse_Project;
GO


/* ============================================================
   SILVER LAYER – CUSTOMER DATA CLEANING & STANDARDIZATION
   ------------------------------------------------------------
   Source Table : bronze.crm_cust_info
   Target Table : silver.crm_cust_info
   Purpose:
   - Deduplicate customer records
   - Clean text fields
   - Standardize gender and marital status values
   - Prepare consistent customer master data
   ============================================================ */


/* ============================================================
   Step 1: Initial Data Review
   ------------------------------------------------------------
   Inspect raw CRM customer data before applying transformations.
   ============================================================ */
SELECT *
FROM bronze.crm_cust_info;


/* ============================================================
   Step 2: Check for NULLs and Duplicate Business Keys
   ------------------------------------------------------------
   cst_id acts as the business identifier for customers.
   Identify NULL or duplicate cst_id values.
   ============================================================ */
SELECT
    cst_id,
    COUNT(*) AS record_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
    OR cst_id IS NULL;


/* ============================================================
   Step 3: Identify Latest Record per Customer
   ------------------------------------------------------------
   Use ROW_NUMBER to rank records per customer based on
   creation date, keeping the most recent record.
   ============================================================ */
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY cst_id
        ORDER BY cst_create_date DESC
    ) AS flag_last
FROM bronze.crm_cust_info;


/* ============================================================
   Step 4: Review Duplicate Records (Older Versions)
   ------------------------------------------------------------
   Records where flag_last != 1 represent older duplicates
   that will be excluded from Silver layer.
   ============================================================ */
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last != 1;


/* ============================================================
   Step 5: Extract Latest Valid Customer Records
   ------------------------------------------------------------
   These records represent the most current version of
   each customer and will be retained.
   ============================================================ */
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1;


/* ============================================================
   Step 6: Check for Unwanted Leading/Trailing Spaces
   ------------------------------------------------------------
   Identify inconsistent text values that require trimming.
   ============================================================ */
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


/* ============================================================
   Step 7: Trim Text Fields & Deduplicate Records
   ------------------------------------------------------------
   Clean name fields and keep only the latest record per
   customer using ROW_NUMBER logic.
   ============================================================ */
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1;


/* ============================================================
   Step 8: Review Distinct Categorical Values
   ------------------------------------------------------------
   Understand current values before standardization.
   ============================================================ */
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


/* ============================================================
   Step 9: Standardize Gender & Marital Status
   ------------------------------------------------------------
   Convert short codes into business-friendly values.
   Handle unknown or inconsistent values safely.
   ============================================================ */
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status, -- Normalize marital status values
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr, -- Normalize gender values
    cst_create_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;


/* ============================================================
   Step 10: Post-Load Validation
   ------------------------------------------------------------
   Validate cleaned and standardized data after inserting
   into the Silver layer table.
   ============================================================ */
SELECT *
FROM silver.crm_cust_info;


/* ============================================================
   SILVER LAYER – PRODUCT DATA CLEANING & TRANSFORMATION
   ------------------------------------------------------------
   Source Table : bronze.crm_prd_info
   Target Table : silver.crm_prd_info
   Purpose:
   - Validate product identifiers
   - Prepare product keys for downstream joins
   - Handle null and invalid values
   - Standardize product attributes
   - Correct product effective date ranges
   ============================================================ */


/* ============================================================
   Step 1: Check for NULLs and Duplicate Product IDs
   ------------------------------------------------------------
   prd_id is treated as the business identifier for products.
   Identify duplicate or missing product records.
   ============================================================ */
SELECT 
    prd_id,
    COUNT(*) AS record_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 
    OR prd_id IS NULL;


/* ============================================================
   Step 2: Validate Product Category Key Mapping
   ------------------------------------------------------------
   Extract category ID from the first part of prd_key and
   verify it exists in ERP category reference table.
   This ensures referential consistency for future joins.
   ============================================================ */
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Category ID for ERP join
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
    SELECT DISTINCT id 
    FROM bronze.erp_px_cat_g1v2
);


/* ============================================================
   Step 3: Validate Product Key Usage in Sales
   ------------------------------------------------------------
   Extract the product identifier portion of prd_key and
   confirm it is referenced in CRM sales transactions.
   ============================================================ */
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Product key for sales join
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
    SELECT sls_prd_key 
    FROM bronze.crm_sales_details
);


/* ============================================================
   Step 4: Check for Invalid or Missing Product Costs
   ------------------------------------------------------------
   Identify negative or NULL product cost values that
   need correction before loading into Silver layer.
   ============================================================ */
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 
   OR prd_cost IS NULL;


/* ============================================================
   Step 5: Handle NULL Product Costs
   ------------------------------------------------------------
   Replace NULL product costs with 0 to ensure numeric
   stability for downstream aggregations.
   ============================================================ */
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost, -- Default NULL cost to 0
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;


/* ============================================================
   Step 6: Review Product Line Values
   ------------------------------------------------------------
   Inspect distinct product line codes prior to standardization.
   ============================================================ */
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;


/* ============================================================
   Step 7: Standardize Product Line Codes
   ------------------------------------------------------------
   Convert short product line codes into business-readable
   values for reporting and analytics.
   ============================================================ */
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line, -- Normalize product line
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;


/* ============================================================
   Step 8: Identify Invalid Product Date Ranges
   ------------------------------------------------------------
   Check for records where the product end date occurs
   before the start date.
   ============================================================ */
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


/* ============================================================
   Step 9: Analyze Product Versioning Using LEAD
   ------------------------------------------------------------
   Use LEAD to derive the correct end date based on the
   next product version's start date.
   ============================================================ */
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) 
        OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


/* ============================================================
   Step 10: Final Transformed Output for Silver Layer
   ------------------------------------------------------------
   - Standardized product attributes
   - Cleaned numeric fields
   - Corrected effective date ranges
   - Ready for insertion into silver.crm_prd_info
   ============================================================ */
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT *
FROM silver.crm_prd_info;


/* ============================================================
   SILVER LAYER – SALES DATA CLEANING & TRANSFORMATION
   ------------------------------------------------------------
   Source Table : bronze.crm_sales_details
   Target Table : silver.crm_sales_details
   Purpose:
   - Clean invalid date values stored as integers
   - Enforce logical date ordering
   - Validate and correct sales, quantity, and price values
   - Produce consistent transactional data for analytics
   ============================================================ */


/* ============================================================
   Step 1: Identify Invalid Order Dates
   ------------------------------------------------------------
   Dates are stored as integers (YYYYMMDD).
   Values <= 0 cannot be converted into valid DATE types.
   ============================================================ */
SELECT
    sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;


/* ============================================================
   Step 2: Identify Invalid Order Date Formats & Ranges
   ------------------------------------------------------------
   Conditions checked:
   - Zero or negative values
   - Incorrect length (not 8 digits)
   - Dates outside acceptable business range
   ============================================================ */
SELECT
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101;


/* ============================================================
   Step 3: Validate Shipping Dates
   ------------------------------------------------------------
   Apply the same validation logic used for order dates.
   ============================================================ */
SELECT
    NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
   OR LEN(sls_ship_dt) != 8
   OR sls_ship_dt > 20500101
   OR sls_ship_dt < 19000101;


/* ============================================================
   Step 4: Validate Due Dates
   ------------------------------------------------------------
   Ensure due dates fall within acceptable date boundaries.
   ============================================================ */
SELECT
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
   OR LEN(sls_due_dt) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;


/* ============================================================
   Step 5: Check Logical Date Ordering
   ------------------------------------------------------------
   Business rules:
   - Order date must not be after ship date
   - Order date must not be after due date
   ============================================================ */
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;


/* ============================================================
   Step 6: Convert Integer Dates to DATE Data Type
   ------------------------------------------------------------
   - Invalid or malformed dates are converted to NULL
   - Valid YYYYMMDD values are cast to DATE
   ============================================================ */
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;


/* ============================================================
   Step 7: Validate Sales, Quantity, and Price Consistency
   ------------------------------------------------------------
   Business rule:
   - sales = quantity * price
   Additional checks:
   - No NULL, zero, or negative values
   ============================================================ */
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


/* ============================================================
   Step 8: Apply Business Rules to Correct Sales & Price
   ------------------------------------------------------------
   Rules:
   - If sales is NULL, zero, negative, or inconsistent,
     derive it using quantity * ABS(price)
   - If price is NULL or zero, calculate it using sales / quantity
   - Negative prices are converted to positive values
   ============================================================ */
SELECT DISTINCT
    sls_sales        AS old_sls_sales,
    sls_quantity,
    sls_price        AS old_sls_price,
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    CASE 
        WHEN sls_price IS NULL 
          OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


/* ============================================================
   Step 9: Final Transformed Output for Silver Layer
   ------------------------------------------------------------
   - Cleaned and validated dates
   - Corrected sales and pricing values
   - Ready for insertion into silver.crm_sales_details
   ============================================================ */
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL 
          OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;


/* ============================================================
   Step 10: Post-Load Validation
   ------------------------------------------------------------
   Verify data after loading into Silver layer.
   ============================================================ */
SELECT *
FROM silver.crm_sales_details;


/* ============================================================
   SILVER LAYER – ERP CUSTOMER DATA CLEANING & STANDARDIZATION
   ------------------------------------------------------------
   Source Table : bronze.erp_cust_az12
   Target Table : silver.erp_cust_az12
   Purpose:
   - Prepare ERP customer attributes for integration
   - Normalize customer identifiers for cross-system joins
   - Validate and clean birthdate values
   - Standardize gender values
   ============================================================ */


/* ============================================================
   Step 1: Initial Data Review
   ------------------------------------------------------------
   Inspect raw ERP customer data before transformations.
   ============================================================ */
SELECT
    cid,
    bdate,
    gen
FROM bronze.erp_cust_az12;


/* ============================================================
   Step 2: Cross-System Join Validation
   ------------------------------------------------------------
   Review CRM customer data to ensure ERP customer IDs
   can be aligned with CRM customer keys in later joins.
   ============================================================ */
SELECT *
FROM silver.crm_cust_info;


/* ============================================================
   Step 3: Normalize Customer Identifier (cid)
   ------------------------------------------------------------
   Some ERP customer IDs are prefixed with 'NAS'.
   Remove this prefix to align with CRM customer keys.
   ============================================================ */
SELECT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12;


/* ============================================================
   Step 4: Identify Out-of-Range Birthdates
   ------------------------------------------------------------
   Business rules:
   - Birthdate must not be unrealistically old
   - Birthdate must not be in the future
   ============================================================ */
SELECT DISTINCT
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();


/* ============================================================
   Step 5: Clean Birthdate Values
   ------------------------------------------------------------
   - Future dates are converted to NULL
   - Valid historical dates are retained
   ============================================================ */
SELECT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    gen
FROM bronze.erp_cust_az12;


/* ============================================================
   Step 6: Review Gender Values
   ------------------------------------------------------------
   Inspect distinct gender values prior to standardization.
   ============================================================ */
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;


/* ============================================================
   Step 7: Standardize Gender Values
   ------------------------------------------------------------
   Convert inconsistent gender codes into standardized,
   business-friendly values.
   ============================================================ */
SELECT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;


/* ============================================================
   Step 8: Post-Load Validation
   ------------------------------------------------------------
   Verify cleaned and standardized ERP customer data
   after loading into Silver layer.
   ============================================================ */
SELECT *
FROM silver.erp_cust_az12;


/* ============================================================
   SILVER LAYER – ERP LOCATION DATA CLEANING & STANDARDIZATION
   ------------------------------------------------------------
   Source Table : bronze.erp_loc_a101
   Target Table : silver.erp_loc_a101
   Purpose:
   - Prepare customer location data for analytics
   - Normalize customer identifiers for joins
   - Standardize country values
   - Handle missing and inconsistent location data
   ============================================================ */


/* ============================================================
   Step 1: Initial Data Review
   ------------------------------------------------------------
   Inspect raw ERP location data before transformations.
   ============================================================ */
SELECT 
    cid,
    cntry
FROM bronze.erp_loc_a101;


/* ============================================================
   Step 2: Validate Join Keys Against CRM Customers
   ------------------------------------------------------------
   Review CRM customer keys to ensure ERP customer IDs
   can be aligned correctly during integration.
   ============================================================ */
SELECT cst_key
FROM silver.crm_cust_info;


/* ============================================================
   Step 3: Normalize Customer Identifier
   ------------------------------------------------------------
   Remove hyphens from customer IDs to ensure consistency
   across ERP and CRM systems.
   ============================================================ */
SELECT 
    REPLACE(cid, '-', '') AS cid,
    cntry
FROM bronze.erp_loc_a101;


/* ============================================================
   Step 4: Review Country Code Variations
   ------------------------------------------------------------
   Identify inconsistent or abbreviated country values
   before standardization.
   ============================================================ */
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


/* ============================================================
   Step 5: Standardize Country Values
   ------------------------------------------------------------
   Convert country codes and inconsistent values into
   business-friendly, standardized country names.
   ============================================================ */
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;


/* ============================================================
   Step 6: Post-Load Validation
   ------------------------------------------------------------
   Verify cleaned and standardized location data after
   loading into the Silver layer.
   ============================================================ */
SELECT *
FROM silver.erp_loc_a101;


/* ============================================================
   SILVER LAYER – ERP PRODUCT CATEGORY DATA CLEANING
   ------------------------------------------------------------
   Source Table : bronze.erp_px_cat_g1v2
   Target Table : silver.erp_px_cat_g1v2
   Purpose:
   - Prepare product category reference data
   - Validate text consistency
   - Ensure clean lookup values for joins in Gold layer
   ============================================================ */


/* ============================================================
   Step 1: Initial Data Review
   ------------------------------------------------------------
   Inspect raw ERP product category data before transformations.
   ============================================================ */
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;


/* ============================================================
   Step 2: Check for Unwanted Leading or Trailing Spaces
   ------------------------------------------------------------
   Validate that category fields do not contain extra spaces
   that could break joins or filters.
   ============================================================ */
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);
-- No records returned indicates clean text values


/* ============================================================
   Step 3: Review Distinct Category Values
   ------------------------------------------------------------
   Confirm that category names are consistent and standardized.
   ============================================================ */
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;


/* ============================================================
   Step 4: Review Distinct Subcategory Values
   ------------------------------------------------------------
   Validate subcategory consistency for analytical grouping.
   ============================================================ */
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;


/* ============================================================
   Step 5: Review Maintenance Flag Values
   ------------------------------------------------------------
   Ensure maintenance indicators are consistent and usable
   for downstream business logic.
   ============================================================ */
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
-- No inconsistencies found


/* ============================================================
   Step 6: Post-Load Validation
   ------------------------------------------------------------
   Verify ERP product category data after loading into
   Silver layer.
   ============================================================ */
SELECT *
FROM silver.erp_px_cat_g1v2;



