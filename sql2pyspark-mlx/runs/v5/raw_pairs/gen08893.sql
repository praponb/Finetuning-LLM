WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS event_seq
    FROM inventory
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.store_id_max_quantity,
    s.event_seq,
    (SELECT MAX(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS max_store_sale_units
FROM scored s;
