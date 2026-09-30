SELECT
    category,
    cost,
    stock_qty,
    DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_date_label
FROM products
WHERE price <= 680
