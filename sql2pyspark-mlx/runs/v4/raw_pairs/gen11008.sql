WITH unpacked AS (
    SELECT
        id,
        created_at,
        COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown,
        COALESCE(attributes['gift'], 'none') AS gift_or_default,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text
    FROM carts
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    COUNT(*) AS num_rows,
    MIN(created_at) AS first_seen,
    COUNT(*) AS num_rows_2,
    GROUPING(DATE_TRUNC('DAY', created_at)) AS gid
FROM unpacked
GROUP BY ROLLUP (DATE_TRUNC('DAY', created_at))
HAVING num_rows > 1
ORDER BY num_rows_2 DESC;
