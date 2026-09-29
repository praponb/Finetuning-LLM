WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        attributes,
        COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'weight_kg') AS has_weight_kg,
        ARRAY_CONTAINS(coupon_codes, 'VIP') AS used_vip,
        FORALL(items, x -> x.qty <= 5) AS all_small_qty,
        ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price
    FROM carts
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    attributes,
    city_or_unknown,
    has_weight_kg,
    used_vip,
    all_small_qty,
    max_item_price,
    CUME_DIST() OVER (ORDER BY max_item_price) AS max_item_price_cume_dist,
    LEAD(max_item_price, 1) OVER (PARTITION BY customer_id ORDER BY max_item_price, id) AS next_max_item_price,
    ROUND(max_item_price / SUM(max_item_price) OVER (PARTITION BY customer_id), 4) AS max_item_price_share,
    COUNT(max_item_price) OVER (PARTITION BY customer_id) AS customer_id_count_max_item_price,
    LAG(max_item_price, 2) OVER (ORDER BY max_item_price, id) AS prev_max_item_price
FROM unpacked
