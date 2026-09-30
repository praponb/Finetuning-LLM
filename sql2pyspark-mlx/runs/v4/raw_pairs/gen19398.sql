WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE age IS NOT NULL
),
summary AS (
    SELECT CASE WHEN city IN ('Singapore', 'Berlin') THEN 'group_a' ELSE 'other' END AS city_group, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
    FROM filtered_customers
    GROUP BY CASE WHEN city IN ('Singapore', 'Berlin') THEN 'group_a' ELSE 'other' END
)
SELECT city_group, avg_credit_limit
FROM summary
WHERE avg_credit_limit > 23
ORDER BY avg_credit_limit DESC
