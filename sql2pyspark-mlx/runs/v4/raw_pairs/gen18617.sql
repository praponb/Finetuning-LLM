SELECT
    s.city,
    s.country,
    p.product_name,
    p.price
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
WHERE p.stock_qty = 310 AND s.supplier_name LIKE 'To%';
