WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount, 0) AS amount_rounded,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label,
        COALESCE(merchant, 'N/A') AS merchant_filled
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 11 AND 18
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_label,
        merchant_filled,
        TO_DATE(txn_time) AS txn_time_date,
        CAST(txn_time AS DATE) AS txn_time_day
    FROM valid
    WHERE merchant IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior,
        MAX(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d,
        NTILE(5) OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS amount_bucket,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d
    FROM filtered
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY running_amount, id) AS rn
    FROM metrics
    QUALIFY rn = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM latest
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_label,
        merchant_filled,
        txn_time_date,
        txn_time_day,
        amount_max_7d,
        amount_sum_7d,
        SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', txn_time_date) AS activity_month,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    ROUND(AVG(amount), 2) AS avg_amount
FROM sessions_transactions
GROUP BY DATE_TRUNC('MONTH', txn_time_date)
ORDER BY avg_amount DESC
