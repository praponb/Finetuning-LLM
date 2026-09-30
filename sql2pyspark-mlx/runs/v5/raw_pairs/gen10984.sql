WITH unpacked AS (
    SELECT
        id,
        customer_id,
        items,
        ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
        FORALL(items, item -> item.qty <= 5) AS all_small_qty,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        attributes['gift'] AS gift
    FROM carts
)
SELECT
    customer_id,
    APPROX_PERCENTILE(max_item_price, 0.5) AS max_item_price_p50,
    APPROX_PERCENTILE(max_item_price, 0.95) AS max_item_price_p95,
    ROUND(AVG(max_item_price), 2) AS avg_max_item_price,
    PERCENTILE_APPROX(max_item_price, 0.9) AS max_item_price_p90,
    ROUND(AVG(max_item_price), 2) AS avg_max_item_price_2
FROM unpacked
GROUP BY customer_id
HAVING ROUND(AVG(max_item_price), 2) < 1000000
ORDER BY avg_max_item_price DESC
