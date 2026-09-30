WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_bucket,
        MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity
    FROM inventory
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity_bucket,
    (SELECT SUM(t2.units) FROM store_sales t2 WHERE t2.store_id = s.store_id) AS sum_store_sale_units
FROM with_windows s;
