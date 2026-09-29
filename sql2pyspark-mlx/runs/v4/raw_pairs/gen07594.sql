WITH with_windows AS (
    SELECT
        *,
        AVG(amount) OVER w_recent AS amount_avg_6_prior,
        SUM(amount) OVER (PARTITION BY txn_type, account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share_2,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS event_seq
    FROM transactions
    WINDOW w_recent AS (PARTITION BY merchant, txn_type ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    txn_type,
    amount,
    merchant,
    is_flagged,
    running_amount,
    amount_share,
    amount_share_2,
    event_seq
FROM with_windows
