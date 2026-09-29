SELECT
    id,
    txn_type,
    amount,
    account_id,
    txn_time,
    RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS amount_rank,
    LAG(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_amount
FROM transactions
WHERE merchant = 'Shell'
