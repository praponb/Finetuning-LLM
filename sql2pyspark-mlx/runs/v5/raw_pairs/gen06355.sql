WITH latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.status,
        top1.txn_type
    FROM accounts s
    JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.amount < 2600
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        DENSE_RANK() OVER (PARTITION BY txn_type, customer_id ORDER BY balance NULLS FIRST) balance_dense_rank,
        AVG(balance) OVER (PARTITION BY txn_type ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) balance_avg_3,
        SUM(balance) OVER w_running running_balance,
        AVG(balance) OVER w_part balance_avg_2
    FROM eligible
    WHERE customer_id IS NOT NULL
    WINDOW w_running AS (PARTITION BY txn_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM accounts
    WHERE status IN ('OPEN', 'CLOSED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_type IN ('DEBIT', 'FEE')
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance_dense_rank,
        s.balance_avg_2,
        c.credit_limit,
        c.age
    FROM kept s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
kept_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND NOT c.is_flagged
        )
)
-- Join a dimension and aggregate
SELECT
    c.country,
    c.segment,
    a.account_type,
    COUNT(*) AS rows_total,
    AVG(a.credit_limit) AS avg_credit_limit,
    GROUPING_ID() AS level_id
FROM kept_accounts a
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY CUBE (c.country, c.segment, a.account_type)
