SELECT
    id,
    credit_limit,
    email,
    LAST_VALUE(email) OVER (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_email
FROM customers
