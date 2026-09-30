SELECT
    i.store_id,
    p.stock_qty,
    s.rating,
    s.country
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
RIGHT OUTER JOIN suppliers s ON p.supplier_id = s.id;
