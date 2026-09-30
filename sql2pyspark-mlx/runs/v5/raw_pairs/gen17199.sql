WITH joined AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        p2.balance,
        p2.opened_date,
        p2.account_type
    FROM transactions a
    INNER JOIN accounts p2
        ON a.account_id = p2.id
),
enriched AS (
    -- Attach accounts attributes
    SELECT a.*, p2.balance AS account_balance
    FROM joined a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY opened_date DESC, id ASC) = 1
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        d.status,
        d.opened_date AS account_opened_date,
        d.account_type AS account_account_type
    FROM deduped e
    LEFT JOIN accounts d
        ON e.account_id = d.id
    WHERE d.status != 'CLOSED'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY balance DESC) AS rn
    FROM enriched_transactions
    QUALIFY rn = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched_transactions_2 AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        p.status AS account_status,
        p.account_type AS account_account_type_2
    FROM islands s
    INNER JOIN accounts p
        ON s.account_id = p.id
)
-- Window metrics over several different partitions
SELECT
    id,
    account_id,
    amount,
    txn_time,
    merchant,
    is_flagged,
    balance,
    session_start_flag,
    burst_id,
    account_account_type_2,
    MIN(balance) OVER (PARTITION BY status ORDER BY txn_time ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_balance,
    AVG(balance) OVER (PARTITION BY status ORDER BY txn_time ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_balance,
    COUNT(balance) OVER (PARTITION BY status ORDER BY txn_time ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_balance,
    MAX(balance) OVER (PARTITION BY status ORDER BY txn_time ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_balance,
    MAX(balance) OVER (ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS balance_max_30d
FROM enriched_transactions_2;
