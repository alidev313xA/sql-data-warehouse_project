/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Dimension: gold.dim_date (2000–2030)
-- =============================================================================
IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO

CREATE VIEW gold.dim_date AS
WITH Numbers AS (
    SELECT TOP (DATEDIFF(DAY, '2000-01-01', '2030-12-31') + 1)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM master.dbo.spt_values a
    CROSS JOIN master.dbo.spt_values b
)
SELECT
    DATEADD(DAY, n, '2000-01-01') AS date,
    DATEPART(YEAR, DATEADD(DAY, n, '2000-01-01')) AS year,
    DATEPART(QUARTER, DATEADD(DAY, n, '2000-01-01')) AS quarter,
    DATEPART(MONTH, DATEADD(DAY, n, '2000-01-01')) AS month,
    DATENAME(MONTH, DATEADD(DAY, n, '2000-01-01')) AS month_name,
    DATEPART(WEEK, DATEADD(DAY, n, '2000-01-01')) AS week_of_year,
    DATEPART(DAY, DATEADD(DAY, n, '2000-01-01')) AS day,
    DATENAME(WEEKDAY, DATEADD(DAY, n, '2000-01-01')) AS weekday_name,
    CASE 
        WHEN DATENAME(WEEKDAY, DATEADD(DAY, n, '2000-01-01')) IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM Numbers;
GO

-- Create a physical table (recommended for real warehouses)
SELECT *
INTO gold.dim_date_table
FROM gold.dim_date;

-- =============================================================================
-- Create Dimension: gold.dim_location
-- =============================================================================
IF OBJECT_ID('gold.dim_location', 'V') IS NOT NULL
    DROP VIEW gold.dim_location;
GO

CREATE VIEW gold.dim_location AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cntry) AS location_key,  -- Surrogate key
    la.cntry             AS country,
    CASE 
        WHEN la.cntry = 'Germany' THEN 'Berlin'
        WHEN la.cntry = 'United States' THEN 'New York'
        WHEN la.cntry = 'France' THEN 'Paris'
		WHEN la.cntry = 'Australia' THEN 'Sydney'
        ELSE 'Unknown City'
    END                AS city,
    CASE 
        WHEN la.cntry = 'Germany' THEN 'Europe'
        WHEN la.cntry = 'United States' THEN 'North America'
        WHEN la.cntry = 'France' THEN 'Europe'
		WHEN la.cntry = 'Australia' THEN 'Oceana'
        ELSE 'Unknown Region'
    END                AS region
FROM silver.erp_loc_a101 la;
GO    
    
-- =============================================================================
-- Update Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num      AS order_number,
    pr.product_key      AS product_key,
    cu.customer_key     AS customer_key,
    lo.location_key     AS location_key,
    dd.date             AS order_date,
    sd.sls_ship_dt      AS shipping_date,
    sd.sls_due_dt       AS due_date,
    sd.sls_sales        AS sales_amount,
    sd.sls_quantity     AS quantity,
    sd.sls_price        AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_location lo
    ON cu.country = lo.country
LEFT JOIN gold.dim_date_table dd
    ON CAST(sd.sls_order_dt AS DATE) = dd.date
GO
