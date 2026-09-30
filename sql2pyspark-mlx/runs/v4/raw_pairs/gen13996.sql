SELECT
    i.id,
    i.product_id,
    p.stock_qty,
    p.launch_date,
    s.country
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'eco')
ORDER BY product_id DESC, id
LIMIT 10;
