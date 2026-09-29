SELECT customer_id, account_type
FROM accounts
EXCEPT
SELECT customer_id, account_type
FROM accounts
WHERE account_type = 'CHECKING';
