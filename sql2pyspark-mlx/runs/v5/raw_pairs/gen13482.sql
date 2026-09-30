WITH lines AS (
    SELECT
        id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, v
)
SELECT
    key,
    COUNT(*) AS row_count,
    COUNT_IF(key = 'device') AS device_events
FROM lines
GROUP BY key
ORDER BY device_events DESC NULLS LAST;
