SELECT
    dim.country,
    dim.city,
    src.category,
    COUNT(*) AS num_rows,
    AVG(src.cost) AS avg_cost
FROM products src
JOIN suppliers dim ON src.supplier_id = dim.id
GROUP BY dim.country, dim.city, src.category
