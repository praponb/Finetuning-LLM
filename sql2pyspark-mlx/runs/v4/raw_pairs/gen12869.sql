WITH unpacked AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        ship_to,
        attributes,
        ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price,
        ship_to.city AS ship_city,
        TRANSFORM(
            FILTER(items, x -> x.promo IS NOT NULL),
            x -> STRUCT(x.product_id AS pid, ROUND(x.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'weight_kg') AS has_weight_kg
    FROM carts
    WHERE customer_id IS NULL
)
SELECT
    *,
    LAST_VALUE(ship_city, TRUE) OVER (PARTITION BY ship_city ORDER BY max_item_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_ship_city,
    ROW_NUMBER() OVER w_part event_seq
FROM unpacked
WINDOW w_part AS (PARTITION BY customer_id ORDER BY max_item_price ASC, id ASC);
