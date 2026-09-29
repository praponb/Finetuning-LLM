SELECT *
FROM (
    SELECT is_active, AVG(credit_limit) AS avg_credit_limit
    FROM customers
    GROUP BY is_active
) AS agg
WHERE avg_credit_limit >= 6600
ORDER BY avg_credit_limit DESC;
