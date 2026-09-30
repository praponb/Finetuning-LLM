WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        ship_to,
        attributes,
        attributes['utm_source'] AS utm_source,
        TRANSFORM(
            FILTER(items, x -> x.promo IS NOT NULL),
            x -> STRUCT(x.product_id AS pid, ROUND(x.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines
    FROM carts
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(*) AS num_rows,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(*) AS num_rows_2
FROM unpacked
GROUP BY hour_window
ORDER BY span_minutes NULLS LAST;
