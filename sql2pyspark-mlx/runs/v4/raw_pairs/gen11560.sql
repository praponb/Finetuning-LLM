WITH account_totals AS (
    SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    at.stddev_amount,
    at.shell_count
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
