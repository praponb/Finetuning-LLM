SELECT
    s.rating,
    s.city,
    p.launch_date,
    p.cost,
    p.supplier_id
FROM suppliers s
JOIN products p ON p.supplier_id = s.id;
