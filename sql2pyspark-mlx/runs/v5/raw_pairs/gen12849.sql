SELECT
    p.brand,
    COUNT(*) AS num_rows,
    SUM(src.units) AS total_units,
    AVG(src.revenue) AS avg_revenue
FROM store_sales src
INNER JOIN products p ON src.product_id = p.id
GROUP BY p.brand;
