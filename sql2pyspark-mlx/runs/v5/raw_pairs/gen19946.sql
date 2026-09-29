SELECT
    id,
    name,
    signup_date,
    is_active,
    credit_limit
FROM (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC NULLS FIRST) AS rn
    FROM customers
) t
WHERE rn = 1
