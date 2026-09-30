WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        INITCAP(TRIM(merchant)) AS merchant_title,
        COALESCE(merchant, 'DIRECT') AS merchant_clean
    FROM transactions
    WHERE txn_time > DATE '2024-05-06'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
remaining_keys AS (
    -- Set difference on keys
    SELECT account_id, id
    FROM islands
    EXCEPT
    SELECT account_id, id
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 10 AND 15
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id AND s.id = k.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT merchant, id
    FROM kept_rows
    EXCEPT
    SELECT merchant, id
    FROM transactions
    WHERE txn_time >= '2024-04-29'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN matched_keys k ON s.merchant = k.merchant AND s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_time,
        merchant_clean,
        session_id,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM reconciled
    WHERE merchant IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_hist AS is_flagged_flag_cume_dist,
        CUME_DIST() OVER (ORDER BY session_id DESC) AS session_id_cume_dist,
        MAX(is_flagged_flag) OVER w_part AS is_flagged_flag_max_30d,
        AVG(is_flagged_flag) OVER w_part AS is_flagged_flag_avg_30d,
        SUM(is_flagged_flag) OVER w_recent AS is_flagged_flag_sum_90d
    FROM valid
    WINDOW w_hist AS (ORDER BY is_flagged_flag DESC NULLS FIRST), w_part AS (ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(is_flagged_flag_max_30d) OVER (ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS is_flagged_flag_max_30d_avg_90d,
        DENSE_RANK() OVER (ORDER BY is_flagged_flag_cume_dist DESC NULLS FIRST) AS is_flagged_flag_cume_dist_dense_rank,
        RANK() OVER (ORDER BY is_flagged_flag_avg_30d DESC NULLS FIRST) AS is_flagged_flag_avg_30d_rank
    FROM with_windows
)
-- Grouped metrics
SELECT
    session_id,
    MAX(txn_time) AS last_txn_time,
    COUNT(*) AS row_count,
    ROUND(AVG(is_flagged_flag_avg_30d), 2) AS avg_is_flagged_flag_avg_30d
FROM with_windows_transactions
GROUP BY 1;
