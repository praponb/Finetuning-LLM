WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, value
),
joined AS (
    SELECT src.*, d.signup_date
    FROM flattened src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
joined_carts AS (
    SELECT
        src.*,
        d.credit_limit,
        d.country
    FROM joined src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
flagged AS (
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY country ORDER BY created_at, id, attr_key) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM joined_carts
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    PERCENTILE_APPROX(burst_id, 0.5) AS burst_id_p50,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    PERCENTILE_APPROX(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles
FROM sessionized
GROUP BY CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('HOUR', created_at)
ORDER BY hour_window NULLS FIRST;
