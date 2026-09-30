WITH account_totals AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
    FROM transactions
    WHERE account_id IN (1, 39)
    GROUP BY account_id
)
SELECT accounts.customer_id, accounts.balance, account_totals.credit_count
FROM accounts
JOIN account_totals ON accounts.id = account_totals.account_id
WHERE account_totals.credit_count > 37
ORDER BY account_totals.credit_count DESC
