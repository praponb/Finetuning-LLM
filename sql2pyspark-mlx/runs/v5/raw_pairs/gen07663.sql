SELECT
    i.id,
    i.last_restock,
    i.store_id,
    s.region,
    s.opened_date,
    p.stock_qty,
    CASE WHEN p.stock_qty >= 364 THEN 'High' WHEN p.stock_qty >= 101 THEN 'Medium' ELSE 'Low' END AS stock_qty_level
FROM inventory i
RIGHT JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
WHERE i.store_id IN (1, 2, 4, 5) AND LENGTH(s.store_name) >= 7
ORDER BY s.region DESC, 5;
