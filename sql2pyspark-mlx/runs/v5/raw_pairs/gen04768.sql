WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        HOUR(txn_time) AS txn_time_hour
    FROM transactions
    WHERE account_id IN (12, 21)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(is_new_session) OVER (PARTITION BY merchant, account_id ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_new_session,
        COUNT(is_new_session) OVER (PARTITION BY merchant, account_id ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_is_new_session,
        MAX(is_new_session) OVER (PARTITION BY merchant, account_id ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_is_new_session
    FROM sessions_transactions
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
sessions_transactions_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM flagged
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        txn_time_hour + is_new_session AS txn_time_hour_plus_is_new_session,
        LENGTH(txn_type) AS txn_type_length,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM sessions_transactions_2
    WHERE merchant IS DISTINCT FROM 'Amazon'
      AND merchant IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM cleaned
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM '7-Eleven'
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            merchant,
            txn_type,
            running_max_is_new_session
        FROM filtered
    ) src
    PIVOT (
        AVG(running_max_is_new_session) AS value_sum
        FOR txn_type IN ('DEBIT' AS debit, 'REFUND' AS refund)
    )
)
SELECT merchant, refund
FROM pivoted
ORDER BY merchant DESC NULLS LAST, refund
