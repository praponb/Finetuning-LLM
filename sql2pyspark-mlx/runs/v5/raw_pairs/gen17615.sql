WITH with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_quantity,
        x.last_restock
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.product_name,
    s.cost,
    s.tags,
    s.supplier_id,
    s.inventory_quantity,
    CASE WHEN product_name RLIKE '^(Office Chair|Desk Lamp)$' THEN 'PRIORITY' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'GROUP_B' ELSE 'CORE' END AS product_name_zone,
    (SELECT MAX(t2.price) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS max_product_price
FROM with_last s
ORDER BY inventory_quantity DESC;
