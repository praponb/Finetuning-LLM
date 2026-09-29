WITH parsed AS (
    SELECT
        id,
        customer_id,
        items,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'utm_source') AS has_utm_source,
        TRANSFORM(items, item -> item.product_id) AS product_ids
    FROM carts
)
SELECT
    customer_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count,
    COUNT(*) AS event_count_2
FROM parsed
GROUP BY customer_id;
