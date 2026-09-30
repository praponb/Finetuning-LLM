WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        AVG(balance) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior,
        LEAD(balance, 2) OVER (PARTITION BY customer_id, status ORDER BY opened_date, id) AS next_balance,
        LAST_VALUE(balance, TRUE) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM accounts
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY opened_date DESC) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.txn_type
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    txn_type,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(next_balance, ARRAY(0.25, 0.5, 0.75)) AS next_balance_quartiles,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    ROUND(SUM(last_known_balance) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS last_known_balance_per_closed
FROM kept
GROUP BY DATE_TRUNC('QUARTER', opened_date), DATE_TRUNC('WEEK', opened_date), txn_type
ORDER BY txn_type DESC;
