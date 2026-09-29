WITH unpacked AS (
    SELECT
        id,
        customer_id,
        items,
        ship_to,
        attributes,
        COALESCE(attributes['device'], 'unknown') AS device_or_default,
        COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown,
        FORALL(items, li -> li.qty <= 5) AS all_small_qty,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device,
        EXISTS(items, li -> li.price > 100) AS has_premium_item
    FROM carts
    WHERE customer_id IN (22, 24)
)
SELECT
    customer_id,
    COUNT(*) AS cnt,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids_2,
    COUNT(DISTINCT device_or_default) AS distinct_device_or_defaults
FROM unpacked
GROUP BY customer_id
ORDER BY distinct_customer_ids_2
