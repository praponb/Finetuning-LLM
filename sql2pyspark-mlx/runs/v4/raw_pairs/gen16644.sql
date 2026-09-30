SELECT a.id, a.account_type, a.customer_id
FROM accounts a
WHERE a.id IN (SELECT account_id FROM transactions WHERE TO_DATE(txn_time) >= '2023-06-01')
ORDER BY customer_id DESC;
