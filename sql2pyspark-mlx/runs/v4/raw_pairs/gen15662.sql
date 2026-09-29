WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
),
flagged AS (
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY k ORDER BY created_at, id, k) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM exploded
),
sessions_carts AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
matched AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM sessions_carts a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.created_at BETWEEN sub.start_date AND sub.end_date
)
SELECT
    CASE WHEN burst_id >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    PERCENTILE_APPROX(monthly_fee, 0.95) AS monthly_fee_p95,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM matched
GROUP BY CASE WHEN burst_id >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY burst_id_tier DESC NULLS LAST;
