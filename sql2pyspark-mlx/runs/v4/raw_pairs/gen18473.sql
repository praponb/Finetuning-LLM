SELECT
    s.supplier_name,
    p.cost,
    p.launch_date,
    p.price
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
ORDER BY price DESC;
