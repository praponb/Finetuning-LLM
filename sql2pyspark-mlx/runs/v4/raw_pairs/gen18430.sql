WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        NTILE(3) OVER w_ordered AS amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS event_seq
    FROM transactions
    WINDOW w_ordered AS (PARTITION BY account_id ORDER BY amount DESC, id)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount_bucket DESC, id) = 1
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2,
        NTILE(4) OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS amount_bucket_2,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        FIRST_VALUE(amount_bucket, TRUE) OVER (PARTITION BY merchant ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount_bucket, id) AS seq_no
    FROM latest
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        amount_avg_2 - running_amount AS amount_avg_2_minus_running_amount,
        COALESCE(seq_no, 0) AS seq_no_filled
    FROM with_windows_transactions
    WHERE amount_bucket_2 > 6
),
with_windows_transactions_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount_bucket) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_bucket_sum_90d,
        AVG(amount_bucket) OVER (PARTITION BY txn_type ORDER BY amount_avg_2_minus_running_amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_bucket_avg_3_prior
    FROM filtered
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            account_id,
            txn_type,
            amount_bucket_2
        FROM with_windows_transactions_2
    ) src
    PIVOT (
        MAX(amount_bucket_2) AS total,
        COUNT(1) AS item_count
        FOR txn_type IN ('FEE' AS fee, 'DEBIT' AS debit, 'REFUND' AS refund)
    )
)
SELECT
    account_id,
    fee_total,
    debit_total,
    debit_item_count,
    refund_total,
    refund_item_count
FROM pivoted
ORDER BY refund_item_count;
