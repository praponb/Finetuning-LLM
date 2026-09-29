WITH flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    SELECT
        id,
        txn_type,
        merchant,
        is_flagged,
        is_new_session,
        SUM(session_no) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_session_no,
        AVG(session_no) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_session_no,
        MAX(session_no) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_session_no
    FROM islands
),
kept_keys AS (
    SELECT txn_type, merchant
    FROM enriched
    INTERSECT
    SELECT txn_type, merchant
    FROM transactions
    WHERE NOT is_flagged
),
verified AS (
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY running_max_session_no DESC) AS running_max_session_no_cume_dist,
        CUME_DIST() OVER (ORDER BY is_new_session NULLS FIRST) AS is_new_session_cume_dist
    FROM verified
),
enriched_transactions AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY is_new_session DESC NULLS FIRST) AS is_new_session_cume_dist_2,
        STDDEV_SAMP(running_max_session_no) OVER (PARTITION BY txn_type) AS txn_type_stddev_samp_running_max_session_no
    FROM metrics
    WHERE is_new_session > 1
),
metrics_transactions AS (
    SELECT
        id,
        txn_type,
        is_new_session_cume_dist,
        is_new_session_cume_dist_2,
        txn_type_stddev_samp_running_max_session_no,
        CUME_DIST() OVER w_part AS running_max_session_no_cume_dist_cume_dist,
        STDDEV_SAMP(txn_type_stddev_samp_running_max_session_no) OVER (PARTITION BY merchant, txn_type) AS merchant_stddev_samp_txn_type_stddev_samp_running_max_session_no
    FROM enriched_transactions
    WINDOW w_part AS (PARTITION BY txn_type ORDER BY running_max_session_no_cume_dist NULLS FIRST)
),
valid AS (
    SELECT
        id,
        txn_type,
        is_new_session_cume_dist,
        txn_type_stddev_samp_running_max_session_no,
        running_max_session_no_cume_dist_cume_dist,
        ROUND(is_new_session_cume_dist, 2) AS is_new_session_cume_dist_rounded
    FROM metrics_transactions
    WHERE merchant_stddev_samp_txn_type_stddev_samp_running_max_session_no < 6
)
SELECT
    txn_type,
    ROUND(SUM(running_max_session_no_cume_dist_cume_dist) / NULLIF(COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END), 0), 2) AS running_max_session_no_cume_dist_cume_dist_per_debit,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN running_max_session_no_cume_dist_cume_dist ELSE 0 END) AS credit_running_max_session_no_cume_dist_cume_dist
FROM valid
GROUP BY txn_type;
