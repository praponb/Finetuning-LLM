WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        COALESCE(balance, 0) AS balance_filled,
        COALESCE(status, 'N/A') AS status_clean
    FROM accounts
    WHERE opened_date >= DATE '2023-06-28'
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.opened_date,
        s.status,
        s.balance_filled,
        s.status_clean,
        top1.transaction_txn_time,
        top1.merchant
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.transaction_txn_time_2
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time_2
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.transaction_txn_time
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        opened_date,
        balance_filled,
        transaction_txn_time,
        merchant,
        transaction_txn_time_2
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY opened_date DESC, id) AS dedup_rank
        FROM not_flagged
    ) x
    WHERE dedup_rank = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(transaction_txn_time) OVER (PARTITION BY merchant ORDER BY transaction_txn_time, id) IS NULL
                 OR transaction_txn_time > LAG(transaction_txn_time) OVER (PARTITION BY merchant ORDER BY transaction_txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM deduped
),
sessions_accounts AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        transaction_txn_time,
        merchant,
        transaction_txn_time_2,
        SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY transaction_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        transaction_txn_time,
        CUME_DIST() OVER (PARTITION BY merchant ORDER BY session_no) AS session_no_cume_dist,
        AVG(session_no) OVER (PARTITION BY merchant ORDER BY session_no, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS session_no_avg_5_prior,
        AVG(session_no) OVER (PARTITION BY merchant ORDER BY transaction_txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS session_no_avg_2
    FROM sessions_accounts
    WHERE TO_DATE(transaction_txn_time_2) >= '2025-02-01'
)
-- Ranking and gap features
SELECT
    id,
    session_no_avg_2,
    COUNT(session_no_avg_5_prior) OVER w_recent AS last3_count_session_no_avg_5_prior,
    MAX(session_no_avg_5_prior) OVER w_recent AS last3_max_session_no_avg_5_prior,
    AVG(session_no_avg_5_prior) OVER w_recent AS last3_avg_session_no_avg_5_prior
FROM enriched
WINDOW w_recent AS (ORDER BY session_no_cume_dist, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
