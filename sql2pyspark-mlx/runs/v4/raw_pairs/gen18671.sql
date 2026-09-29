WITH parsed AS (
    SELECT
        id,
        customer_id,
        ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
        COALESCE(attributes['device'], 'none') AS device_or_default
    FROM carts
)
SELECT
    customer_id,
    APPROX_PERCENTILE(max_item_price, 0.5) AS max_item_price_p50,
    PERCENTILE_APPROX(max_item_price, 0.9) AS max_item_price_p90,
    SUM(max_item_price) AS total_max_item_price,
    MAX(max_item_price) AS max_max_item_price
FROM parsed
GROUP BY customer_id
ORDER BY max_max_item_price DESC NULLS LAST
