WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE credit_limit >= 14600
),
agg AS (
    SELECT is_active, SUM(age) AS total_age, MAX(credit_limit) AS max_credit_limit
    FROM filtered_customers
    GROUP BY is_active
)
SELECT is_active, total_age, max_credit_limit
FROM agg
WHERE max_credit_limit > 10200
ORDER BY max_credit_limit DESC;
