SELECT
    s.city,
    s.supplier_name,
    s.rating,
    p.cost,
    p.launch_date,
    p.category
FROM suppliers s
INNER JOIN products p ON s.id = p.supplier_id
WHERE p.cost > 540
