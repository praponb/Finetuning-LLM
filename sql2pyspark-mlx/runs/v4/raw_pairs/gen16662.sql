SELECT
    a.balance,
    a.status,
    t.total_amount,
    t.fee_count
FROM accounts a
LEFT JOIN (SELECT account_id, SUM(amount) total_amount, SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END) fee_count
    FROM transactions
    WHERE merchant != 'Grab'
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.fee_count > 53
ORDER BY a.balance DESC
