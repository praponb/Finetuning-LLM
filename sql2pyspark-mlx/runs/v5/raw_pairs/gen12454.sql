SELECT
    id,
    credit_limit,
    signup_date,
    email,
    COALESCE(email, 'N/A') AS email_filled
FROM customers
ORDER BY id DESC, email_filled DESC
LIMIT 20;
