WITH RecentCustomers AS (
    SELECT *
    FROM customers
    WHERE city IN ('Chiang Mai', 'New York', 'Tokyo') AND country IN ('TH', 'SG')
),
is_active_summary AS (
    SELECT is_active, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
    FROM RecentCustomers
    GROUP BY is_active
)
SELECT is_active, avg_credit_limit
FROM is_active_summary
ORDER BY avg_credit_limit, is_active
LIMIT 3
