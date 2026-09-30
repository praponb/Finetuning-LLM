SELECT
    i.last_restock,
    i.quantity,
    i.product_id,
    p.launch_date,
    p.price,
    p.stock_qty
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id;
