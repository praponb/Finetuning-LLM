SELECT
    d.region,
    d.city,
    COUNT(*) AS rows_total,
    MAX(a.quantity) AS max_quantity,
    GROUPING_ID() AS level_id
FROM inventory a
INNER JOIN stores d ON a.store_id = d.id
GROUP BY GROUPING SETS ((d.region, d.city), (d.region), ())
