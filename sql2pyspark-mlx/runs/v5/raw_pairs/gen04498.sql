WITH lines AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price
    FROM (
        SELECT
            id,
            customer_id,
            coupon_codes,
            EXPLODE_OUTER(items) AS elem
        FROM carts
    ) t
),
lines_carts AS (
    SELECT
        id,
        qty,
        price,
        tag
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS tag
),
remaining_keys AS (
    SELECT id
    FROM lines_carts
    EXCEPT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
verified AS (
    SELECT s.*
    FROM lines_carts s
    JOIN remaining_keys k ON s.id = k.id
),
metrics AS (
    SELECT *, ROUND(qty / SUM(qty) OVER (PARTITION BY tag), 4) AS qty_share
    FROM verified
)
SELECT
    CASE WHEN price >= 190 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END AS qty_tier,
    PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    SIZE(COLLECT_SET(tag)) AS num_tags,
    COUNT(*) AS cnt
FROM metrics
GROUP BY CASE WHEN price >= 190 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY qty_tier NULLS LAST
