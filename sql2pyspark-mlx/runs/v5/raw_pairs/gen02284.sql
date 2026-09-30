WITH transactions_by_account AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    a.opened_date,
    tba.amount_range,
    tba.avg_amount
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY avg_amount DESC
