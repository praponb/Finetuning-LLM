SELECT
    transactions.id,
    transactions.is_flagged,
    accounts.status,
    customers.credit_limit,
    customers.country
FROM transactions
INNER JOIN accounts ON transactions.account_id = accounts.id
INNER JOIN customers ON customers.id = accounts.customer_id
