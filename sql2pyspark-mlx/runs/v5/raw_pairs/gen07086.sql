WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE txn_type IN ('DEBIT', 'FEE', 'CREDIT')
),
prev_events AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM valid
),
flagged AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
flagged_2 AS (
    SELECT *, CASE WHEN LAG(prev_txn_time) OVER (PARTITION BY txn_type ORDER BY prev_txn_time, id) IS NULL
                 OR prev_txn_time > LAG(prev_txn_time) OVER (PARTITION BY txn_type ORDER BY prev_txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM islands
),
sessions_transactions AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        resolved_merchant,
        prev_txn_time,
        burst_id,
        SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY prev_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
),
valid_transactions AS (
    SELECT *, ROUND(burst_id, 2) AS burst_id_rounded
    FROM sessions_transactions
    WHERE prev_txn_time >= '2023-10-18'
),
valid_transactions_2 AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        resolved_merchant,
        prev_txn_time,
        ROUND(burst_id, 0) AS burst_id_rounded_2
    FROM valid_transactions
    WHERE burst_id > 3
),
joined AS (
    SELECT
        s.*,
        d.account_type,
        d.opened_date
    FROM valid_transactions_2 s
    LEFT JOIN accounts d
        ON s.account_id = d.id
)
SELECT
    CASE WHEN burst_id_rounded_2 >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_rounded_2_tier,
    COUNT(*) AS event_count,
    COUNT(*) AS event_count_2,
    ROUND(SUM(burst_id_rounded_2) / NULLIF(COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END), 0), 2) AS burst_id_rounded_2_per_refund,
    COUNT(*) AS num_rows,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    SUM(CASE WHEN account_type = 'CHECKING' THEN burst_id_rounded_2 ELSE 0 END) AS checking_burst_id_rounded_2
FROM joined
GROUP BY CASE WHEN burst_id_rounded_2 >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY checking_burst_id_rounded_2;
