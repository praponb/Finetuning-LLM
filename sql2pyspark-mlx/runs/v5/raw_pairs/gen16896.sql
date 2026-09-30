WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        elem.product_id,
        elem.price
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            EXPLODE(items) AS elem
        FROM carts
    ) t
)
SELECT
    product_id,
    customer_id,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(DISTINCT id) AS distinct_ids,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min
FROM lines
GROUP BY CUBE (product_id, customer_id, DATE_TRUNC('HOUR', created_at))
ORDER BY product_id DESC;
