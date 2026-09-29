WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        ADD_MONTHS(last_restock, 6) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock >= DATE '2023-07-06'
)
SELECT
    DATE_TRUNC('WEEK', last_restock_plus_months) AS week_start,
    store_id,
    product_id,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    COUNT(DISTINCT store_id) AS distinct_store_ids_2,
    COUNT(*) AS event_count,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', last_restock_plus_months) ORDER BY COUNT(*) DESC) AS event_count_dense_rank
FROM cleaned
GROUP BY DATE_TRUNC('WEEK', last_restock_plus_months), store_id, product_id
ORDER BY distinct_product_ids DESC;
