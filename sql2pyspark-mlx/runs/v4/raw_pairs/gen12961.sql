WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        coupon_codes,
        items,
        ship_to,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10,
        ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
        TRANSFORM(
            FILTER(items, item -> item.promo IS NOT NULL),
            item -> STRUCT(item.product_id AS pid, ROUND(item.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        FORALL(items, item -> item.qty <= 5) AS all_small_qty,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
    FROM carts
    WHERE SIZE(coupon_codes) >= 2
)
-- Running and moving aggregates
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY max_item_price NULLS LAST) AS max_item_price_dense_rank,
    AVG(weight_kg) OVER (ORDER BY max_item_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weight_kg_avg_6,
    RANK() OVER (ORDER BY weight_kg DESC) AS weight_kg_rank,
    CUME_DIST() OVER w_ordered AS weight_kg_cume_dist,
    LAG(max_item_price, 1) OVER (ORDER BY max_item_price, id) AS prev_max_item_price
FROM flattened_attrs
WINDOW w_ordered AS (ORDER BY weight_kg DESC NULLS FIRST)
