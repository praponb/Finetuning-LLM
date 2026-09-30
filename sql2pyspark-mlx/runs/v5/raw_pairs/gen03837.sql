WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        balance,
        status,
        AVG(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS balance_avg_3_prior
    FROM accounts
    WHERE balance > 70000
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount BETWEEN 2200 AND 3700
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(balance_avg_3) OVER (PARTITION BY customer_id) AS customer_id_max_balance_avg_3,
        LAST_VALUE(status, TRUE) OVER (PARTITION BY status, customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status
    FROM eligible
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.txn_time
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (36, 38)
        )
),
stats AS (
    -- Multi-level aggregation
    SELECT
        status,
        COUNT_IF(status = 'OPEN') AS open_events,
        AVG(balance_avg_3_prior) AS avg_balance_avg_3_prior,
        MIN(txn_time) AS first_seen,
        COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count
    FROM not_flagged
    GROUP BY status
)
SELECT
    status,
    avg_balance_avg_3_prior,
    first_seen,
    open_count
FROM stats
ORDER BY avg_balance_avg_3_prior NULLS LAST;
