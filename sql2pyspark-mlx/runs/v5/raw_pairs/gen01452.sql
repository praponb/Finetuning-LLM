WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, value
)
SELECT
    k,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    MIN(created_at) AS first_seen,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list_2
FROM lines
GROUP BY k, activity_day
ORDER BY activity_day DESC NULLS FIRST;
