WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', status, account_type) AS status_account_type_key,
        COALESCE(status, 'NONE') AS resolved_status
    FROM accounts
    WHERE status IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY opened_date DESC NULLS FIRST, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, account_type
    FROM ranked
    EXCEPT ALL
    SELECT id, account_type
    FROM accounts
    WHERE customer_id = 34
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.account_type = k.account_type
),
filtered_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(balance * 1.1, 2) AS balance_adjusted,
        COALESCE(status, 'NONE') AS status_filled
    FROM reconciled
    WHERE opened_date > DATE '2023-05-18'
      AND status IN ('OPEN', 'FROZEN')
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN balance_adjusted >= 81 THEN 'LARGE' ELSE 'SMALL' END AS balance_adjusted_tier,
        customer_id,
        COUNT(*) FILTER (WHERE status = 'OPEN') AS open_rows,
        COUNT(DISTINCT account_type) AS distinct_account_types,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN balance_adjusted >= 81 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(*) FILTER (WHERE status = 'OPEN') DESC) AS open_rows_dense_rank
    FROM filtered_accounts
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT account_type) > 1
)
SELECT
    s.balance_adjusted_tier,
    s.customer_id,
    s.open_rows,
    s.distinct_account_types,
    s.open_rows_dense_rank,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM totals s;
