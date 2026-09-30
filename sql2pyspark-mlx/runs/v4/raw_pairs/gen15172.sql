SELECT
    i.last_restock,
    i.store_id,
    p.stock_qty,
    p.supplier_id
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
ORDER BY p.stock_qty DESC;
