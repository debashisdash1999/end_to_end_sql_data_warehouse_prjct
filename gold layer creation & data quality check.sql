/* ============================================================
   DATABASE CONTEXT
   ------------------------------------------------------------
   Sets the active database for creating Gold layer views.
   ============================================================ */
USE Datawarehouse_Project;
GO


/* ============================================================
   GOLD LAYER VIEW: gold.dim_customers
   ------------------------------------------------------------
   View Type : DIMENSION

   Why Dimension:
   - Contains descriptive attributes about customers
   - Used to slice, filter, and group sales facts
   - Does not store transactional or additive measures
   - Low to medium cardinality

   Grain:
   - One row per unique customer

   Business Role:
   - Provides customer context for sales analytics
   - Supports customer demographics and geography analysis

   Source Tables:
   - silver.crm_cust_info   (System of record for customers)
   - silver.erp_cust_az12   (Additional demographic attributes)
   - silver.erp_loc_a101    (Customer location data)
   ============================================================ */

CREATE VIEW gold.dim_customers AS

/* ------------------------------------------------------------
   Step 1: Generate Surrogate Key
   Why:
   - Surrogate keys are required for star schema modeling
   - Decouples analytics from source system keys
------------------------------------------------------------- */
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,

/* ------------------------------------------------------------
   Step 2: Select Core Customer Identifiers
   Why:
   - CRM is the authoritative source for customer identity
------------------------------------------------------------- */
    ci.cst_id        AS customer_id,
    ci.cst_key       AS customer_number,

/* ------------------------------------------------------------
   Step 3: Select Descriptive Customer Attributes
   Why:
   - Used for reporting, filtering, and segmentation
------------------------------------------------------------- */
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    la.cntry         AS country,
    ci.cst_marital_status AS marital_status,

/* ------------------------------------------------------------
   Step 4: Resolve Gender from Multiple Sources
   Why:
   - CRM is preferred when available
   - ERP is used as a fallback for missing CRM values
------------------------------------------------------------- */
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

/* ------------------------------------------------------------
   Step 5: Add Demographic & Audit Attributes
   Why:
   - Birthdate enables age-based analytics
   - Create date supports customer lifecycle analysis
------------------------------------------------------------- */
    ca.bdate         AS birthdate,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci

/* ------------------------------------------------------------
   Step 6: Join ERP Customer Demographics
   Why:
   - ERP enriches CRM data with attributes not stored in CRM
------------------------------------------------------------- */
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid

/* ------------------------------------------------------------
   Step 7: Join ERP Location Data
   Why:
   - Enables geographic reporting and segmentation
------------------------------------------------------------- */
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;


-- Validation: Review customer dimension
SELECT * FROM gold.dim_customers;



/* ============================================================
   GOLD LAYER VIEW: gold.dim_products
   ------------------------------------------------------------
   View Type : DIMENSION

   Why Dimension:
   - Contains descriptive product attributes
   - Used to analyze sales by category, product line, etc.
   - No transactional measures

   Grain:
   - One row per active product

   Business Role:
   - Provides product context for sales analytics

   Source Tables:
   - silver.crm_prd_info
   - silver.erp_px_cat_g1v2
   ============================================================ */

CREATE VIEW gold.dim_products AS

/* ------------------------------------------------------------
   Step 1: Generate Product Surrogate Key
   Why:
   - Required for star schema joins
   - Independent of source product identifiers
------------------------------------------------------------- */
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

/* ------------------------------------------------------------
   Step 2: Select Product Identifiers & Attributes
------------------------------------------------------------- */
    pn.prd_id   AS product_id,
    pn.prd_key  AS product_number,
    pn.prd_nm   AS product_name,

/* ------------------------------------------------------------
   Step 3: Add Category & Maintenance Attributes
   Why:
   - Enables category-level reporting and analysis
------------------------------------------------------------- */
    pn.cat_id   AS category_id,
    pc.cat      AS category,
    pc.subcat   AS subcategory,
    pc.maintenance AS maintenance,

/* ------------------------------------------------------------
   Step 4: Add Cost & Classification Attributes
------------------------------------------------------------- */
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn

/* ------------------------------------------------------------
   Step 5: Join Product Category Reference Data
------------------------------------------------------------- */
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id

/* ------------------------------------------------------------
   Step 6: Filter Active Products Only
   Why:
   - Historical products are excluded from current analytics
------------------------------------------------------------- */
WHERE pn.prd_end_dt IS NULL;


-- Validation: Review product dimension
SELECT * FROM gold.dim_products;



/* ============================================================
   GOLD LAYER VIEW: gold.fact_sales
   ------------------------------------------------------------
   View Type : FACT

   Why Fact:
   - Stores measurable business events (sales transactions)
   - Contains numeric, additive metrics
   - References dimensions using surrogate keys

   Grain:
   - One row per sales transaction line item

   Business Role:
   - Central table for revenue and sales performance analysis

   Source Tables:
   - silver.crm_sales_details
   - gold.dim_customers
   - gold.dim_products
   ============================================================ */

CREATE VIEW gold.fact_sales AS

/* ------------------------------------------------------------
   Step 1: Select Transaction Identifiers & Dates
------------------------------------------------------------- */
SELECT
    sd.sls_ord_num  AS order_number,

/* ------------------------------------------------------------
   Step 2: Resolve Dimension Foreign Keys
   Why:
   - Enables star schema joins
   - Improves query performance for analytics
------------------------------------------------------------- */
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,

/* ------------------------------------------------------------
   Step 3: Add Transaction Dates
------------------------------------------------------------- */
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,

/* ------------------------------------------------------------
   Step 4: Add Sales Measures
   Why:
   - Core metrics for revenue and sales analysis
------------------------------------------------------------- */
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price

FROM silver.crm_sales_details sd

/* ------------------------------------------------------------
   Step 5: Join Product Dimension
------------------------------------------------------------- */
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number

/* ------------------------------------------------------------
   Step 6: Join Customer Dimension
------------------------------------------------------------- */
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;


-- Validation: Review fact table
SELECT * FROM gold.fact_sales;


USE Datawarehouse_Project;
GO


/* ============================================================================
   GOLD LAYER – DATA QUALITY & INTEGRITY CHECKS
   ----------------------------------------------------------------------------
   Script Purpose:
   This script performs critical data quality validations on the Gold layer
   to ensure the data model is reliable for analytics and reporting.

   These checks validate:
   - Uniqueness of surrogate keys in dimension tables
   - Correctness of dimensional modeling principles
   - Readiness of Gold layer data for BI consumption

   Why This Matters:
   - Duplicate surrogate keys break star schema joins
   - BI tools assume one-to-many relationships between
     dimensions and facts
   - These checks act as a final gate before data is exposed
     to business users

   Execution Notes:
   - This script is typically run after Gold layer refresh
   - Any returned records indicate a data quality issue
   - Results must be investigated before production use
============================================================================ */


/* ============================================================================
   QUALITY CHECK 1: gold.dim_customers
   ----------------------------------------------------------------------------
   Check Type : Surrogate Key Uniqueness
   Table     : gold.dim_customers

   Why This Check Exists:
   - customer_key is a surrogate key generated in the Gold layer
   - It must uniquely identify each customer record
   - Duplicate surrogate keys cause incorrect aggregations
     and broken joins in fact tables

   Expected Result:
   - No rows returned
============================================================================ */
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


/* ============================================================================
   QUALITY CHECK 2: gold.dim_products
   ----------------------------------------------------------------------------
   Check Type : Surrogate Key Uniqueness
   Table     : gold.dim_products

   Why This Check Exists:
   - product_key is the surrogate key used in the fact table
   - Each product must map to exactly one product_key
   - Duplicate product keys would lead to double counting
     in sales analytics

   Expected Result:
   - No rows returned
============================================================================ */
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

/* ============================================================
   DATA QUALITY CHECK – FOREIGN KEY INTEGRITY
   ------------------------------------------------------------
   Purpose:
   - Ensure all fact records successfully link to dimensions
   - Detect orphaned records before analytics consumption
   ============================================================ */

-- Check missing customer dimension references
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL;

-- Check missing product dimension references
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL;
