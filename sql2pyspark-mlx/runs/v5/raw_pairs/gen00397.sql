WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN balance >= 85000 THEN 'High' WHEN balance >= 21000 THEN 'Medium' ELSE 'Low' END AS balance_level
    FROM accounts
    WHERE customer_id IS NOT NULL
),
valid_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', account_type, status) AS account_type_status_key,
        COALESCE(status, 'NONE') AS status_filled
    FROM valid
    WHERE opened_date >= DATE '2023-04-25'
      AND customer_id IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS row_num
        FROM valid_accounts
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY account_type ORDER BY balance, id) AS balance_bucket,
        COUNT(balance) OVER (PARTITION BY customer_id) AS customer_id_count_balance,
        COUNT(balance) OVER (PARTITION BY customer_id) AS customer_id_count_balance_2
    FROM ranked
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY customer_id_count_balance DESC NULLS FIRST, id) AS row_num_2
    FROM metrics
    QUALIFY row_num_2 = 1
)
-- Join a dimension and aggregate
SELECT
    p.segment,
    p.country,
    a.account_type,
    COUNT(*) AS rows_total,
    MAX(a.balance) AS max_balance
FROM latest a
INNER JOIN customers p ON a.customer_id = p.id
GROUP BY p.segment, p.country, a.account_type;
