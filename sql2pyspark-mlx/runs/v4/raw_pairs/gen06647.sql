SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS amount_dense_rank,
    LEAD(amount, 2) OVER w_running AS next_amount
FROM transactions
WINDOW w_running AS (PARTITION BY account_id ORDER BY amount ASC, id ASC)
