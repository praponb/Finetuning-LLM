WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_6_prior,
        MAX(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_max_monthly_fee
    FROM subscriptions
)
SELECT
    id,
    monthly_fee,
    status,
    running_monthly_fee,
    monthly_fee_avg_6_prior,
    CASE WHEN status RLIKE '^(EXPIRED|ACTIVE)$' THEN 'CORE' WHEN status RLIKE '^(CANCELLED|PAUSED)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS status_zone
FROM scored
ORDER BY id DESC NULLS LAST
LIMIT 10
