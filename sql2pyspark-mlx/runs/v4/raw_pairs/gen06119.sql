WITH parsed AS (
    SELECT
        id,
        created_at,
        ship_to,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines,
        AGGREGATE(items, 0.0D, (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS cart_value
    FROM carts
    WHERE created_at < '2023-05-01'
)
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY cart_value, id) AS event_seq,
    NTILE(10) OVER w_grp AS bulk_lines_bucket,
    LAG(cart_value, 1, 0) OVER w_grp AS prev_cart_value,
    SUM(bulk_lines) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS bulk_lines_sum_30m
FROM parsed
WINDOW w_grp AS (ORDER BY bulk_lines, id)
