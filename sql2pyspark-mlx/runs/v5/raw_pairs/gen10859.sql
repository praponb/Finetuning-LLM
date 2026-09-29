SELECT
    d.city,
    d.region,
    COUNT(*) AS cnt,
    ROUND(AVG(e.discount), 2) AS avg_discount
FROM orders e
INNER JOIN stores d ON e.store_id = d.id
GROUP BY GROUPING SETS ((d.city, d.region), (d.city), ())
