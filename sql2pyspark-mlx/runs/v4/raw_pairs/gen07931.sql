SELECT
    accounts.id,
    accounts.opened_date,
    customers.segment,
    customers.is_active
FROM accounts
INNER JOIN customers ON accounts.customer_id = customers.id
