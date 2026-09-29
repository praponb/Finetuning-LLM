SELECT
    id,
    launch_date,
    product_name,
    category,
    MONTH(launch_date) AS launch_date_month,
    ROUND(stock_qty / cost, 2) AS stock_qty_per_cost
FROM products
WHERE brand = 'Stark';
