WITH account_stats AS (
    SELECT account_id, COUNT(amount) AS amount_count, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    a.balance,
    as.amount_count,
    as.stddev_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id;
