WITH deduped AS (
    SELECT *
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC NULLS LAST, id) = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock,
    (SELECT AVG(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS avg_inventory_quantity
FROM deduped s;
