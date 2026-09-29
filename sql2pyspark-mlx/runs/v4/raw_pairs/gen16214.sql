SELECT
    account_type,
    customer_id,
    SUM(balance) AS total_balance,
    ROUND(STDDEV(balance), 2) AS stddev_balance
FROM accounts
WHERE balance > 35000 AND (status <> 'CLOSED' OR customer_id IN (24, 33))
GROUP BY account_type, customer_id
