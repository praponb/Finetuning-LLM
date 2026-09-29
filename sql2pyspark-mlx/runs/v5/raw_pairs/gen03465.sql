WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS dedup_rank
    FROM inventory
    QUALIFY dedup_rank = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.dedup_rank,
    (SELECT AVG(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS avg_inventory_quantity
FROM ranked s;
