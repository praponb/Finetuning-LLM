WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE status NOT IN ('EXPIRED', 'PAUSED', 'CANCELLED') AND customer_id IN (27, 33)
),
agg AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list, ROUND((100 * SUM(CASE WHEN plan = 'premium' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_premium
    FROM src
    GROUP BY status
)
SELECT status, status_list, pct_premium
FROM agg
WHERE pct_premium >= 62;
