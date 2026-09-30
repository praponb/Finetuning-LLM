SELECT
    *,
    MIN(quantity) OVER (PARTITION BY store_id) AS store_id_min_quantity,
    NTILE(5) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_bucket,
    NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_bucket_2,
    SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
FROM inventory;
