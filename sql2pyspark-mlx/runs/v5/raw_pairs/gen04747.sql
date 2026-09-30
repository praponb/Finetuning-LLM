SELECT
    id,
    credit_limit,
    city,
    segment
FROM customers
WHERE credit_limit >= (SELECT AVG(credit_limit) FROM customers WHERE signup_date > '2024-02-01')
ORDER BY city, id DESC;
