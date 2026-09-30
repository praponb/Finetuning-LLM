WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CONCAT_WS('-', account_type, status) AS account_type_status_key,
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label
    FROM accounts
    WHERE opened_date > DATE '2023-08-09'
),
matched_keys AS (
    -- Set difference on keys
    SELECT status, account_type
    FROM base
    EXCEPT DISTINCT
    SELECT status, account_type
    FROM accounts
    WHERE account_type <> 'SAVINGS'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.status = k.status AND s.account_type = k.account_type
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(balance) AS balance_whole,
        COALESCE(balance, 0) AS balance_filled,
        COALESCE(status, 'N/A') AS status_filled
    FROM verified
    WHERE opened_date > DATE '2023-06-25'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN balance >= 58000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
        APPROX_PERCENTILE(balance_whole, 0.5) AS balance_whole_p50,
        COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END) AS checking_count,
        SIZE(COLLECT_SET(account_type)) AS num_account_types
    FROM not_flagged
    GROUP BY CASE WHEN balance >= 58000 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    balance_tier,
    checking_count,
    num_account_types
FROM grouped
ORDER BY checking_count DESC NULLS FIRST, num_account_types
