WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        coupon_codes,
        items,
        ship_to.country AS ship_country,
        FORALL(items, item -> item.qty <= 5) AS all_small_qty,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    id,
    created_at,
    coupon_codes,
    items,
    ship_country,
    all_small_qty,
    coupons_text,
    FIRST_VALUE(ship_country) IGNORE NULLS OVER (PARTITION BY ship_country ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ship_country,
    FIRST_VALUE(ship_country, TRUE) OVER (PARTITION BY ship_country ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ship_country_2,
    FIRST_VALUE(ship_country, TRUE) OVER (PARTITION BY ship_country ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ship_country_3,
    ROW_NUMBER() OVER (PARTITION BY ship_country ORDER BY created_at, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY ship_country ORDER BY created_at DESC, id) AS event_seq
FROM flattened_attrs
