WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        coupon_codes,
        ship_to,
        ship_to.city AS ship_city,
        ship_to.country AS ship_country,
        AGGREGATE(items, CAST(0 AS DOUBLE), (acc, li) -> acc + li.qty * li.price, acc -> ROUND(acc, 2)) AS cart_value,
        ship_to.zip AS ship_zip,
        attributes['device'] AS device
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
)
-- Running and moving aggregates
SELECT
    *,
    MIN(cart_value) OVER w_part AS running_min_cart_value,
    SUM(cart_value) OVER w_part AS running_sum_cart_value,
    COUNT(cart_value) OVER w_part AS running_count_cart_value,
    SUM(cart_value) OVER (PARTITION BY ship_city ORDER BY cart_value, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cart_value
FROM parsed
WINDOW w_part AS (PARTITION BY ship_country ORDER BY cart_value, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
