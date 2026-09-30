SELECT
    a.id,
    a.opened_date,
    c.name,
    c.city
FROM accounts a
JOIN customers c ON c.id = a.customer_id
WHERE c.signup_date > '2024-03-03' OR a.account_type = 'CHECKING'
