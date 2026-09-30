WITH transactions_by_account AS (
    SELECT account_id, COUNT(amount) amount_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.account_type, a.opened_date, tba.amount_count
FROM accounts a
JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY a.account_type DESC
