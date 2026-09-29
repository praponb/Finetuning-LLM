WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
matched_keys AS (
    -- Set intersection on keys
    SELECT txn_type, merchant
    FROM sessionized
    INTERSECT
    SELECT txn_type, merchant
    FROM transactions
    WHERE TO_DATE(txn_time) >= '2023-04-07'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN matched_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM reconciled
    WHERE txn_time > DATE '2023-07-08'
      AND merchant NOT IN ('7-Eleven', 'Grab', 'Amazon')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        session_no,
        is_flagged_flag,
        merchant_clean,
        is_flagged_flag * session_no AS is_flagged_flag_times_session_no,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM valid
    WHERE txn_time >= TIMESTAMP '2023-08-18 07:00:00'
      AND is_flagged_flag <= 56
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, is_flagged_flag / NULLIF(is_flagged_flag_times_session_no, 0) AS is_flagged_flag_per_is_flagged_flag_times_session_no
    FROM cleaned
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM 'Amazon'
),
kept_keys AS (
    -- Set difference on keys
    SELECT txn_type
    FROM base
    EXCEPT ALL
    SELECT txn_type
    FROM transactions
    WHERE account_id IN (4, 36, 39)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.txn_type = k.txn_type
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(is_flagged_flag_times_session_no, 0) AS is_flagged_flag_times_session_no_rounded,
        CAST(session_no AS DOUBLE) AS session_no_num,
        COALESCE(merchant, 'N/A') AS merchant_filled
    FROM verified
    WHERE txn_time >= DATE '2023-07-21'
      AND merchant IS NOT DISTINCT FROM 'Lazada'
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN is_flagged_flag_times_session_no >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_flagged_flag_times_session_no_tier,
        (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
        ROUND(SUM(is_flagged_flag_times_session_no_rounded) / NULLIF(COUNT(CASE WHEN merchant = 'Grab' THEN 1 END), 0), 2) AS is_flagged_flag_times_session_no_rounded_per_grab,
        COUNT_IF(merchant = 'Shell') AS shell_events
    FROM filtered
    GROUP BY CASE WHEN is_flagged_flag_times_session_no >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    is_flagged_flag_times_session_no_tier,
    span_minutes,
    is_flagged_flag_times_session_no_rounded_per_grab,
    shell_events
FROM daily;
