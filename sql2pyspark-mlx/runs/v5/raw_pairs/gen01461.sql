WITH kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM accounts
    EXCEPT ALL
    SELECT customer_id
    FROM accounts
    WHERE opened_date <= '2024-09-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, account_type
    FROM reconciled
    EXCEPT DISTINCT
    SELECT status, account_type
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN', 'CLOSED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.account_type = k.account_type
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        LOWER(account_type) AS account_type_lower,
        TRIM(account_type) AS clean_account_type
    FROM kept_rows
    WHERE status IS NOT NULL
      AND opened_date >= '2024-04-01'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
)
-- Join a dimension and aggregate
SELECT
    dim.country,
    e.account_type,
    COUNT(*) AS rows_total,
    AVG(e.balance) AS avg_balance,
    GROUPING_ID() AS level_id
FROM kept e
INNER JOIN customers dim ON e.customer_id = dim.id
GROUP BY GROUPING SETS ((dim.country, e.account_type), (dim.country))
ORDER BY account_type DESC NULLS LAST;
