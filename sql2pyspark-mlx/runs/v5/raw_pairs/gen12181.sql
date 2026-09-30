SELECT signup_month, min_credit_limit
FROM (
    SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, MIN(credit_limit) AS min_credit_limit
    FROM customers
    WHERE city = 'Chiang Mai'
    GROUP BY DATE_TRUNC('MONTH', signup_date)
) AS sub
WHERE min_credit_limit > 14200
ORDER BY signup_month DESC
LIMIT 20
