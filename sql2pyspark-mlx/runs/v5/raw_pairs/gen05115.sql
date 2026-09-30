WITH transactions_by_account AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END) amazon_count
    FROM transactions
    WHERE NOT is_flagged
    GROUP BY account_id
)
SELECT a.balance, tba.amazon_count
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY balance DESC
