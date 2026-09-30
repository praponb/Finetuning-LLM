SELECT city, avg_age, min_credit_limit
FROM (
    SELECT city, ROUND(AVG(age), 2) AS avg_age, MIN(credit_limit) AS min_credit_limit
    FROM customers
    WHERE segment IN ('Corporate', 'SMB', 'Consumer')
    GROUP BY city
) AS t
WHERE avg_age < 67
ORDER BY avg_age DESC
