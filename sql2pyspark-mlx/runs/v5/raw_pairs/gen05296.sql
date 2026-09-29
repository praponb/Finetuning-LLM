SELECT
    dim.country,
    a.category,
    COUNT(*) AS cnt,
    MAX(a.cost) AS max_cost,
    GROUPING_ID() AS gid
FROM products a
LEFT JOIN suppliers dim ON a.supplier_id = dim.id
GROUP BY GROUPING SETS ((dim.country, a.category), (dim.country), ());
