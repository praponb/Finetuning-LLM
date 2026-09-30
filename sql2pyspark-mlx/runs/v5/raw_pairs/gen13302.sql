WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        ROW_NUMBER() OVER (ORDER BY balance DESC, id) AS row_seq,
        LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM accounts
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        balance,
        opened_date,
        status,
        row_seq,
        balance / NULLIF(last_known_balance, 0) AS balance_per_last_known_balance
    FROM enriched
    WHERE status IS NOT NULL
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.opened_date,
        s.status,
        s.row_seq,
        s.balance_per_last_known_balance,
        latest.amount
    FROM base s
    JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
)
-- Multi-level aggregation
SELECT
    status,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS row_seq_per_closed,
    COUNT(*) FILTER (WHERE status = 'OPEN') AS open_rows,
    COUNT(*) AS event_count,
    COUNT(DISTINCT status) AS distinct_status,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY COUNT(DISTINCT status) DESC) AS distinct_status_dense_rank
FROM eligible
GROUP BY status, year_start, week_start
ORDER BY open_rows DESC NULLS FIRST
