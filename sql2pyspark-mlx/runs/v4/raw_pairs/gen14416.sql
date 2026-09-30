WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(balance) AS balance_whole
    FROM accounts
    WHERE opened_date >= DATE '2024-02-02'
      AND status IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.country,
        c.email
    FROM base a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT account_type, id
    FROM enriched
    EXCEPT DISTINCT
    SELECT account_type, id
    FROM accounts
    WHERE customer_id = 31
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.account_type = k.account_type AND s.id = k.id
)
SELECT
    id,
    account_type,
    opened_date,
    status,
    balance_whole,
    country,
    CASE WHEN status RLIKE '^(OPEN|CLOSED)$' THEN 'CORE' WHEN status RLIKE '^(FROZEN)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS status_zone
FROM kept_rows
ORDER BY balance_whole NULLS LAST
