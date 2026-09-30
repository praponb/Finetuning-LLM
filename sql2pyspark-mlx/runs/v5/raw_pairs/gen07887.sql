WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN merchant IN ('Starbucks', 'Shell') THEN 'Core' ELSE 'Other' END AS merchant_group,
        CEIL(amount) AS amount_whole,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-10-28 03:00:00'
      AND merchant IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount) OVER (PARTITION BY merchant, txn_type ORDER BY amount_whole, id) AS next_amount,
        LAG(amount) OVER (PARTITION BY merchant, txn_type ORDER BY amount_whole, id) AS prev_amount,
        LAG(amount_whole) OVER (PARTITION BY merchant, txn_type ORDER BY amount_whole, id) AS prev_amount_whole,
        ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY amount_whole, id) AS seq_no,
        COUNT(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(amount_whole) IGNORE NULLS OVER (ORDER BY prev_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_whole,
        COUNT(prev_amount_whole) OVER w_recent AS prev_amount_whole_count_7d,
        LAG(next_amount, 2) OVER (PARTITION BY account_id ORDER BY amount_whole, id) AS prev_next_amount,
        SUM(amount_whole) OVER (PARTITION BY account_id, merchant ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_whole
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM enriched
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 4100 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    CASE WHEN amount_whole >= 78 THEN 'LARGE' ELSE 'SMALL' END AS amount_whole_tier,
    COUNT_IF(txn_type = 'REFUND') AS refund_events,
    ROUND(AVG(is_new_burst), 2) AS avg_is_new_burst
FROM sessions_transactions
GROUP BY CASE WHEN amount >= 4100 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', txn_time), CASE WHEN amount_whole >= 78 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(is_new_burst), 2) >= 0
ORDER BY activity_month DESC
