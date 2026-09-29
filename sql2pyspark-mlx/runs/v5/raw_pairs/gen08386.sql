SELECT
    city,
    segment,
    MAX(credit_limit) AS max_credit_limit,
    COUNT(credit_limit) AS credit_limit_count
FROM customers
WHERE signup_date < '2023-07-01' AND signup_date BETWEEN '2023-08-05' AND '2024-06-01'
GROUP BY city, segment
HAVING COUNT(credit_limit) < 2
ORDER BY city, segment
LIMIT 1
