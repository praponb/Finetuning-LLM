SELECT
    d.city,
    COUNT(*) AS num_rows,
    ROUND(AVG(e.units), 2) AS avg_units,
    MAX(e.revenue) AS max_revenue
FROM store_sales e
LEFT JOIN stores d ON e.store_id = d.id
GROUP BY ROLLUP (d.city)
ORDER BY city
