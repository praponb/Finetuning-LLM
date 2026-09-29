SELECT credit_limit, signup_date, CONCAT_WS('/', segment, email) AS segment_email_key
FROM customers
WHERE (email IS NOT NULL AND age <= 49) OR is_active;
