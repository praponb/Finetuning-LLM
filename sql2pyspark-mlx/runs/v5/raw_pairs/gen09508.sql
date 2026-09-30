WITH enriched AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.amount,
        a.txn_time,
        d.balance,
        d.account_type,
        d.status
    FROM transactions a
    JOIN accounts d
        ON a.account_id = d.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(status, ' ', account_type) AS status_account_type,
        DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time
    FROM enriched
    WHERE txn_time > DATE '2023-07-06'
      AND account_type IS NOT NULL
),
with_dims AS (
    -- Attach accounts attributes
    SELECT e.*, a.opened_date
    FROM cleaned e
    LEFT JOIN accounts a
        ON e.account_id = a.id
),
enriched_transactions AS (
    -- Ranking and gap features
    SELECT
        id,
        days_since_txn_time,
        opened_date,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER w_recent AS event_seq_2,
        FIRST_VALUE(days_since_txn_time) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_txn_time
    FROM with_dims
    WINDOW w_running AS (PARTITION BY txn_type ORDER BY txn_time DESC, id ASC), w_recent AS (PARTITION BY account_id, status ORDER BY balance DESC, id ASC)
),
enriched_transactions_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        days_since_txn_time,
        opened_date,
        MIN(event_seq) OVER w_part AS last7_min_event_seq,
        SUM(event_seq) OVER w_part AS last7_sum_event_seq,
        AVG(event_seq) OVER w_part AS last7_avg_event_seq
    FROM enriched_transactions
    WINDOW w_part AS (ORDER BY event_seq ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN last7_min_event_seq >= 7.4 THEN 'HIGH' ELSE 'LOW' END AS last7_min_event_seq_tier,
    CASE WHEN last7_avg_event_seq >= 7.8 THEN 'HIGH' ELSE 'LOW' END AS last7_avg_event_seq_tier,
    SUM(days_since_txn_time) AS total_days_since_txn_time,
    PERCENTILE_APPROX(last7_avg_event_seq, 0.95) AS last7_avg_event_seq_p95,
    PERCENTILE_APPROX(last7_sum_event_seq, 0.95) AS last7_sum_event_seq_p95,
    GROUPING(CASE WHEN last7_min_event_seq >= 7.4 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
FROM enriched_transactions_2
GROUP BY GROUPING SETS ((CASE WHEN last7_min_event_seq >= 7.4 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last7_avg_event_seq >= 7.8 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN last7_min_event_seq >= 7.4 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY last7_avg_event_seq_p95 ASC NULLS LAST
