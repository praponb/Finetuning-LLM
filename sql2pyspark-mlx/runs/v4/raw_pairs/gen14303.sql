WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(txn_time AS DATE) AS txn_time_day,
        FLOOR(amount) AS amount_whole
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-10-25 04:00:00'
      AND txn_type = 'CREDIT'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount_whole) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_whole_avg_6_prior,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type, merchant), 4) AS amount_share,
        NTILE(10) OVER (PARTITION BY txn_type ORDER BY new_group_flag, id) AS new_group_flag_bucket
    FROM sessionized
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        QUARTER(txn_time) AS txn_time_quarter,
        ADD_MONTHS(txn_time_day, 3) AS txn_time_day_plus_months,
        COALESCE(merchant, 'N/A') AS merchant_clean
    FROM scored
    WHERE merchant IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS amount_bucket,
        AVG(txn_time_quarter) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS txn_time_quarter_avg_90d,
        ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY new_group_flag_bucket, id) AS seq_no,
        SUM(amount_share) OVER (PARTITION BY txn_type, merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_share
    FROM cleaned
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time_quarter_avg_90d NULLS LAST, id) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        merchant,
        txn_type,
        amount
    FROM latest
) src
PIVOT (
    MAX(amount) AS value_sum,
    COUNT(1) AS cnt
    FOR txn_type IN ('FEE' AS fee, 'CREDIT' AS credit, 'REFUND' AS refund)
);
