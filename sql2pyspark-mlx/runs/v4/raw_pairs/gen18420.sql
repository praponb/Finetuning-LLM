WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DAYOFWEEK(txn_time) AS txn_time_dayofweek,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE is_flagged
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM islands
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM islands_transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC NULLS FIRST, id) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY prev_txn_time DESC, id) AS rn
    FROM deduped
    QUALIFY rn = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_time_dayofweek,
        resolved_merchant,
        is_new_session,
        island_id,
        prev_txn_time,
        new_group_flag,
        session_no,
        rn,
        LAST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY prev_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
        SUM(new_group_flag) OVER (PARTITION BY account_id) AS account_id_sum_new_group_flag,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY is_new_session, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        AVG(session_no) OVER (PARTITION BY account_id, merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS session_no_avg_7d
    FROM latest
)
-- Grouped metrics
SELECT
    island_id,
    ROUND(SUM(txn_time_dayofweek) / NULLIF(COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END), 0), 2) AS txn_time_dayofweek_per_7_eleven,
    MAX(txn_time) AS last_txn_time,
    COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count,
    MIN(txn_time) AS first_txn_time,
    SUM(session_no) FILTER (WHERE merchant = 'Amazon') AS amazon_total_session_no
FROM enriched
GROUP BY 1
ORDER BY island_id DESC NULLS FIRST
