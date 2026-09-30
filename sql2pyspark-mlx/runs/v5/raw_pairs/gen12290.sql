WITH filtered_subscriptions AS (
    SELECT *, LENGTH(plan) AS plan_length
    FROM subscriptions
    WHERE start_date >= '2023-05-18'
),
agg AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM filtered_subscriptions
    GROUP BY status
)
SELECT status, status_list
FROM agg
ORDER BY status
LIMIT 20;
