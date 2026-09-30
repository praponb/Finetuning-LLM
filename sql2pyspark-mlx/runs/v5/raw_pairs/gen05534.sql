SELECT
    id,
    name,
    email,
    signup_date,
    is_active,
    age
FROM customers
QUALIFY RANK() OVER (PARTITION BY country, segment ORDER BY credit_limit DESC) = 1
