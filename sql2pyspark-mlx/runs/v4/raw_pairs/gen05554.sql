SELECT
    a.account_type,
    a.opened_date,
    a.status,
    c.signup_date
FROM accounts a
INNER JOIN customers c ON c.id = a.customer_id;
