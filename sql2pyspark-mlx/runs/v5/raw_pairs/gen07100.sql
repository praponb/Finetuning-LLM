SELECT segment, MONTH(signup_date) signup_month, AVG(credit_limit) avg_credit_limit
FROM customers
WHERE segment = 'Corporate'
GROUP BY 1, 2
HAVING avg_credit_limit >= 10700
ORDER BY avg_credit_limit, segment, signup_month
LIMIT 10;
