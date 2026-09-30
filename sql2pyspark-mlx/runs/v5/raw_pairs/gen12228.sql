SELECT
    accounts.account_type,
    accounts.balance,
    accounts.status,
    customers.signup_date,
    customers.segment,
    customers.name,
    UPPER(accounts.status) AS status_upper
FROM accounts
INNER JOIN customers ON accounts.customer_id = customers.id;
