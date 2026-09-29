WITH transactions_by_account AS (
    SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.customer_id,
    a.status,
    tba.stddev_amount,
    tba.amount_range
FROM accounts a
LEFT OUTER JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.stddev_amount >= 31;
