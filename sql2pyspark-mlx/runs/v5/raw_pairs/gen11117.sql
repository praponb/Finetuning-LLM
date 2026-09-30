WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        ship_to.country AS ship_country,
        FORALL(items, x -> x.qty <= 5) AS all_small_qty,
        ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price
    FROM carts
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    ship_country,
    all_small_qty,
    COUNT(max_item_price) OVER w_recent AS running_count_max_item_price,
    SUM(max_item_price) OVER w_recent AS running_sum_max_item_price,
    MAX(max_item_price) OVER w_recent AS running_max_max_item_price,
    COUNT(max_item_price) OVER (PARTITION BY customer_id, ship_country ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS max_item_price_count_30m
FROM unpacked
WINDOW w_recent AS (PARTITION BY customer_id ORDER BY max_item_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
