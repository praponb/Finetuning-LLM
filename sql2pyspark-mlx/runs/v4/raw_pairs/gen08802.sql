WITH filtered AS (
    SELECT
        *,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END is_flagged_label,
        IF(is_flagged, 'Yes', 'No') is_flagged_label_2
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-10-02 11:00:00'
      AND merchant IS NOT NULL
      AND amount BETWEEN 1500 AND 2300
),
remaining_keys AS (
    SELECT txn_type, merchant
    FROM filtered
    INTERSECT
    SELECT txn_type, merchant
    FROM transactions
    WHERE account_id IN (7, 21, 39)
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
),
with_dims AS (
    SELECT e.*, a.status
    FROM verified e
    LEFT JOIN accounts a
        ON e.account_id = a.id
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY amount NULLS FIRST) amount_dense_rank,
        MAX(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) amount_max_30d,
        PERCENT_RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) amount_percent_rank
    FROM with_dims
),
base AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        is_flagged_label,
        amount_dense_rank,
        amount_percent_rank,
        amount_dense_rank * amount_max_30d amount_dense_rank_times_amount_max_30d,
        COALESCE(status, 'OTHER') resolved_status
    FROM with_windows
    WHERE txn_time > TIMESTAMP '2024-04-25 04:00:00'
      AND status IS NOT NULL
),
with_dims_transactions AS (
    SELECT
        src.*,
        a.status,
        a.balance,
        a.account_type
    FROM base src
    LEFT JOIN accounts a
        ON src.account_id = a.id
)
SELECT
    s.id,
    s.account_id,
    s.amount,
    s.txn_time,
    s.is_flagged_label,
    s.amount_dense_rank,
    s.amount_percent_rank,
    s.amount_dense_rank_times_amount_max_30d,
    s.resolved_status,
    (SELECT MAX(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS max_transaction_amount
FROM with_dims_transactions s
ORDER BY amount DESC
