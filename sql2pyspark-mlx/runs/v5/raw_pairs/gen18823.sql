WITH with_latest AS (
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.cost,
        s.stock_qty,
        s.launch_date,
        s.supplier_id,
        latest.inventory_last_restock,
        latest.inventory_quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    product_name,
    cost,
    launch_date,
    inventory_quantity
FROM with_latest
ORDER BY inventory_quantity NULLS FIRST, cost DESC;
