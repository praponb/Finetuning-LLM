SELECT
    brand,
    supplier_id,
    MIN(launch_date) first_launch_date,
    ROUND((100 * SUM(CASE WHEN brand = 'Globex' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_globex,
    SUM(CASE WHEN brand = 'Stark' THEN stock_qty ELSE 0 END) stark_stock_qty
FROM products
GROUP BY 1, 2
HAVING SUM(CASE WHEN brand = 'Stark' THEN stock_qty ELSE 0 END) < 21
ORDER BY 2 DESC
