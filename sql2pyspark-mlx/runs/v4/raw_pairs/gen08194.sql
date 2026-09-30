SELECT
    i.product_id,
    i.last_restock,
    p.supplier_id,
    p.stock_qty
FROM inventory i
JOIN products p ON i.product_id = p.id;
