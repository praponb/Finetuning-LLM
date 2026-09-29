WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        CAST(amount AS INT) AS amount_int,
        DATE_FORMAT(txn_time, 'yyyy-MM-dd') AS txn_time_label
    FROM transactions
    WHERE txn_time >= DATE '2023-04-20'
      AND merchant IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
kept_keys AS (
    -- Set difference on keys
    SELECT account_id, txn_type
    FROM sessions_transactions
    EXCEPT
    SELECT account_id, txn_type
    FROM transactions
    WHERE txn_time < '2024-01-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_transactions s
    JOIN kept_keys k ON s.account_id = k.account_id AND s.txn_type = k.txn_type
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        amount / NULLIF(amount_int, 0) AS amount_per_amount_int,
        LEAST(amount_int, amount) AS least_amount_int_amount
    FROM reconciled
    WHERE account_id IN (14, 36)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        amount,
        txn_time,
        txn_time_label,
        session_id,
        amount_per_amount_int,
        SUM(amount_per_amount_int) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_per_amount_int_sum_30d,
        MAX(least_amount_int_amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS least_amount_int_amount_max_7d,
        COUNT(least_amount_int_amount) OVER (PARTITION BY account_id, txn_type) AS account_id_count_least_amount_int_amount,
        DENSE_RANK() OVER w_hist AS amount_per_amount_int_dense_rank,
        STDDEV_SAMP(amount_per_amount_int) OVER w_recent AS account_id_stddev_samp_amount_per_amount_int
    FROM base
    WINDOW w_hist AS (PARTITION BY account_id ORDER BY amount_per_amount_int NULLS FIRST), w_recent AS (PARTITION BY account_id)
),
filtered_transactions AS (
    -- Keep recent, valid rows
    SELECT
        amount,
        txn_time,
        txn_time_label,
        session_id,
        amount_per_amount_int,
        amount_per_amount_int_sum_30d,
        account_id_stddev_samp_amount_per_amount_int,
        ROUND(session_id, 1) AS session_id_rounded
    FROM with_windows
    WHERE txn_time >= TIMESTAMP '2023-05-29 17:00:00'
)
SELECT
    amount,
    txn_time,
    txn_time_label,
    amount_per_amount_int,
    session_id_rounded
FROM filtered_transactions;
