WITH flattened_attrs AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        COALESCE(attributes['device'], 'unknown') AS device_or_default,
        COALESCE(attributes['gift'], 'direct') AS gift_or_default,
        AGGREGATE(items, CAST(0 AS DOUBLE), (acc, li) -> acc + li.qty * li.price, acc -> ROUND(acc, 2)) AS cart_value,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown
    FROM carts
)
SELECT
    CASE WHEN cart_value >= 1670 THEN 'LARGE' ELSE 'SMALL' END AS cart_value_tier,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    COUNT(DISTINCT gift_or_default) AS distinct_gift_or_defaults,
    ROUND(AVG(cart_value), 2) AS avg_cart_value
FROM flattened_attrs
GROUP BY 1
ORDER BY duration_min
