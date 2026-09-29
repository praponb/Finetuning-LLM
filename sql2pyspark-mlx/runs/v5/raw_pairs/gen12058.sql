WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS row_num
    FROM inventory
    QUALIFY row_num = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.row_num,
    (SELECT MIN(t2.shipping_fee) FROM orders t2 WHERE t2.store_id = s.store_id) AS min_order_shipping_fee
FROM ranked s
