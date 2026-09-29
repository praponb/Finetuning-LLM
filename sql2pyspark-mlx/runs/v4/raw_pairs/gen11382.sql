WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        ship_to,
        attributes,
        COALESCE(attributes['device'], 'none') AS device_or_default,
        ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price
    FROM carts
    WHERE customer_id IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN max_item_price >= 190 THEN 'LARGE' ELSE 'SMALL' END AS max_item_price_tier,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    PERCENTILE_APPROX(max_item_price, 0.5) AS max_item_price_p50,
    APPROX_PERCENTILE(max_item_price, 0.9) AS max_item_price_p90,
    PERCENTILE_APPROX(max_item_price, 0.9) AS max_item_price_p90_2
FROM unpacked
GROUP BY CASE WHEN max_item_price >= 190 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY max_item_price_p90_2 NULLS FIRST;
