WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(balance AS INT) balance_int,
        COALESCE(status, 'OTHER') status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-06-24'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY balance, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_type IN ('FEE', 'DEBIT', 'REFUND')
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(balance_int) IGNORE NULLS OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_balance_int,
        LAG(balance, 1) OVER (PARTITION BY status ORDER BY opened_date, id) prev_balance
    FROM eligible
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.status,
    s.balance_int,
    s.first_known_balance_int,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM enriched s;
