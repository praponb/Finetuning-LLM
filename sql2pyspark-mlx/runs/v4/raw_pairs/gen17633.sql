SELECT
    id,
    is_active,
    country,
    name,
    REPLACE(email, ' ', '-') AS email_slug
FROM customers
WHERE signup_date <= '2024-03-17' AND city = 'Singapore' AND signup_date <= '2025-09-01';
