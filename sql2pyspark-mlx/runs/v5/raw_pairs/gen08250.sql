WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_bucket,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM inventory
    WHERE store_id IN (4, 5, 6)
    WINDOW w_part AS (ORDER BY quantity DESC, id)
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.quantity_bucket,
    s.row_seq,
    (SELECT MIN(o2.discount) FROM orders o2 WHERE o2.store_id = s.store_id) AS min_order_discount
FROM metrics s;
