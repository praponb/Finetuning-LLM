WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-07-09 17:00:00'
      AND merchant IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(amount) OVER (PARTITION BY txn_type) AS txn_type_min_amount,
        PERCENT_RANK() OVER w_hist AS amount_percent_rank,
        SUM(amount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM base
    WHERE is_flagged
    WINDOW w_hist AS (ORDER BY amount DESC NULLS LAST)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_type_min_amount, id) = 1
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        amount,
        txn_type_min_amount,
        running_amount,
        ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount_percent_rank DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount_percent_rank, id) AS event_seq
    FROM deduped
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, txn_type
    FROM with_windows_transactions
    INTERSECT ALL
    SELECT id, txn_type
    FROM transactions
    WHERE txn_type IN ('FEE', 'REFUND', 'DEBIT')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_transactions s
    JOIN kept_keys k ON s.id = k.id AND s.txn_type = k.txn_type
)
-- Grouped metrics
SELECT
    txn_type,
    PERCENTILE_APPROX(running_amount, ARRAY(0.25, 0.5, 0.75)) AS running_amount_quartiles,
    ROUND(AVG(amount), 2) AS avg_amount,
    SUM(txn_type_min_amount) FILTER (WHERE txn_type = 'REFUND') AS refund_total_txn_type_min_amount,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(txn_type_min_amount, 0.9) AS txn_type_min_amount_p90,
    SUM(running_amount) FILTER (WHERE txn_type = 'DEBIT') AS debit_total_running_amount
FROM kept_rows
GROUP BY 1;
