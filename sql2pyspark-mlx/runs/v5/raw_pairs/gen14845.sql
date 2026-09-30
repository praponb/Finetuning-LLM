SELECT
    p.id,
    p.price,
    p.stock_qty,
    p.launch_date,
    s.country,
    s.rating,
    s.city
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
