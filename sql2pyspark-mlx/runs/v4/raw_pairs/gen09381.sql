WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN account_type IN ('LOAN', 'CREDIT') THEN 'Tier 1' ELSE 'Other' END AS account_type_group,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-12-09'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY account_type ORDER BY opened_date) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM ranked
    EXCEPT ALL
    SELECT customer_id
    FROM accounts
    WHERE account_type NOT IN ('CREDIT', 'LOAN')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    account_type_group,
    status_filled,
    CASE WHEN account_type RLIKE '^(CHECKING|CREDIT)$' THEN 'PRIORITY' WHEN account_type RLIKE '^(SAVINGS|LOAN)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS account_type_zone
FROM reconciled;
