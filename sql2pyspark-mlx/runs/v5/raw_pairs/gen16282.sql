WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        items,
        attributes,
        attributes['gift'] AS gift,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        SIZE(items) AS num_items,
        COALESCE(attributes['gift'], 'unknown') AS gift_or_default
    FROM carts
)
SELECT
    customer_id,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(num_items, 0.95) AS num_items_p95,
    PERCENTILE_APPROX(num_items, 0.95) AS num_items_p95_2,
    APPROX_PERCENTILE(num_items, 0.5) AS num_items_p50,
    APPROX_PERCENTILE(weight_kg, 0.5) AS weight_kg_p50
FROM flattened_attrs
GROUP BY customer_id
HAVING num_rows > 1
ORDER BY num_items_p50 DESC NULLS LAST;
