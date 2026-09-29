WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        items,
        SIZE(FILTER(items, li -> li.qty >= 3)) AS bulk_lines,
        TRANSFORM(
            FILTER(items, li -> li.promo IS NOT NULL),
            li -> STRUCT(li.product_id AS pid, ROUND(li.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        COALESCE(attributes['gift'], 'none') AS gift_or_default,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
        SIZE(items) AS num_items
    FROM carts
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    PERCENTILE_APPROX(bulk_lines, ARRAY(0.25, 0.5, 0.75)) AS bulk_lines_quartiles,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(bulk_lines, 0.5) AS bulk_lines_p50
FROM unpacked
GROUP BY 1
HAVING COUNT(DISTINCT customer_id) < 1
ORDER BY distinct_customer_ids NULLS FIRST;
