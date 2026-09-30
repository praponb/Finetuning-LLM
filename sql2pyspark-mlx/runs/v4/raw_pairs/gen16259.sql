SELECT
    id,
    signup_date,
    age,
    name,
    UPPER(email) AS email_upper
FROM customers
WHERE credit_limit <= 9600 AND (signup_date <= '2025-02-01' OR is_active = TRUE)
ORDER BY 5
