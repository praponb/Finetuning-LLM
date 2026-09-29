WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY plan ORDER BY monthly_fee NULLS FIRST) AS dedup_rank
    FROM subscriptions
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    dedup_rank,
    CASE WHEN status RLIKE '^(PAUSED|ACTIVE)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(CANCELLED|EXPIRED)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS status_zone
FROM ranked
ORDER BY status
