WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        AVG(amount) OVER (PARTITION BY txn_type, account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        LAG(amount, 1) OVER (ORDER BY txn_time, id) AS prev_amount
    FROM transactions
)
SELECT
    id,
    txn_type,
    txn_time,
    merchant,
    is_flagged,
    amount_avg_6_prior,
    first_known_amount,
    prev_amount,
    CASE WHEN merchant RLIKE '^(Amazon|Grab)$' THEN 'GROUP_B' WHEN merchant RLIKE '^(7-Eleven|Lazada)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS merchant_zone
FROM with_windows;
