-- 1. Total Sales by Product Category
SELECT 
    p.category,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity
FROM gold.fact_sales f
JOIN gold.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;

-- 2. Top 10 Customers by Lifetime Purchase Value
SELECT 
    c.first_name + ' ' + c.last_name AS customer_name,
    c.country,
    SUM(f.sales_amount) AS total_spent,
    COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales f
JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
GROUP BY c.first_name, c.last_name, c.country
ORDER BY total_spent DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;

-- 3. Monthly Sales Trend 
SELECT 
    YEAR(f.order_date) AS year,
    MONTH(f.order_date) AS month,
    SUM(f.sales_amount) AS monthly_sales
FROM gold.fact_sales f
GROUP BY YEAR(f.order_date), MONTH(f.order_date)
ORDER BY year, month;

-- 4. Average Shipping Delay by Product Line
SELECT 
    p.product_line,
    AVG(DATEDIFF(DAY, f.order_date, f.shipping_date)) AS avg_shipping_days
FROM gold.fact_sales f
JOIN gold.dim_products p
    ON f.product_key = p.product_key
WHERE f.shipping_date IS NOT NULL
GROUP BY p.product_line
ORDER BY avg_shipping_days ASC;


-- 5. Profit Margin Analysis by Category
SELECT 
    p.category,
    SUM(f.sales_amount - (p.cost * f.quantity)) AS total_profit,
    SUM(f.sales_amount) AS total_sales,
    ROUND(SUM(f.sales_amount - (p.cost * f.quantity)) * 100.0 / NULLIF(SUM(f.sales_amount), 0), 2) AS profit_margin_percent
FROM gold.fact_sales f
JOIN gold.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY profit_margin_percent DESC;

-- 6. Sales by Country and Category
SELECT 
    c.country,
    p.category,
    SUM(f.sales_amount) AS total_sales,
    COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales f
JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
JOIN gold.dim_products p
    ON f.product_key = p.product_key
GROUP BY c.country, p.category
ORDER BY c.country, total_sales DESC;

