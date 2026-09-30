SELECT
    store_id,
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(*) AS cnt,
    COUNT(*) AS num_rows,
    AVG(quantity) AS avg_quantity,
    RANK() OVER (PARTITION BY store_id ORDER BY COUNT(*) DESC) AS cnt_rank
FROM inventory
GROUP BY store_id, DATE_TRUNC('MONTH', last_restock)
