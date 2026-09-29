SELECT
    accounts.id,
    accounts.opened_date,
    customers.is_active,
    customers.segment,
    customers.signup_date
FROM accounts
LEFT JOIN customers ON accounts.customer_id = customers.id
