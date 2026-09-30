SELECT
    s.id,
    s.supplier_name,
    COUNT(p.id) AS product_count,
    MAX(p.cost) AS max_cost,
    COUNT(CASE WHEN p.brand = 'Stark' THEN 1 END) AS stark_count
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
GROUP BY s.id, s.supplier_name;
