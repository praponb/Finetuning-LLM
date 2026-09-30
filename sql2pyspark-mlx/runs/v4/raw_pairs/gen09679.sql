SELECT customer_id
FROM accounts
WHERE status <> 'FROZEN'
EXCEPT
SELECT customer_id
FROM accounts
WHERE balance > 61000;
