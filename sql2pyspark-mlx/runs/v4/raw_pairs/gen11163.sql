SELECT
    id,
    balance,
    customer_id,
    status
FROM accounts
WHERE balance >= (SELECT MAX(balance) FROM accounts);
