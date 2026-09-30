WITH base AS (
    SELECT *, CONCAT(plan, ', ', status) AS plan_status
    FROM subscriptions
    WHERE plan NOT IN ('enterprise', 'standard', 'basic') AND customer_id IN (31, 35) AND plan != 'enterprise'
),
Summary AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM base
    GROUP BY status
)
SELECT status, status_list
FROM Summary
ORDER BY status;
