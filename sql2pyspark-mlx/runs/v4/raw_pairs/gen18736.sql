SELECT accounts.opened_date, t.total_amount, t.last_txn_time
FROM accounts
INNER JOIN (SELECT account_id, SUM(amount) AS total_amount, MAX(txn_time) AS last_txn_time
    FROM transactions
    WHERE amount <= 2800
    GROUP BY account_id) t ON accounts.id = t.account_id
WHERE t.total_amount >= 12000
ORDER BY t.last_txn_time;
