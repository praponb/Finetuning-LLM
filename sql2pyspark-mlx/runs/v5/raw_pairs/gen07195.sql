WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(opened_date, 'MMM yyyy') opened_date_label
    FROM accounts
    WHERE customer_id IS NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (6, 29)
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT account_type, id
    FROM not_flagged
    INTERSECT
    SELECT account_type, id
    FROM accounts
    WHERE customer_id IN (1, 8, 16, 30)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN matched_keys k ON s.account_type = k.account_type AND s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        account_type,
        opened_date,
        status,
        opened_date_label,
        SUBSTRING(account_type, 1, 3) account_type_prefix
    FROM kept
    WHERE status IN ('FROZEN', 'OPEN', 'CLOSED')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, MONTH(opened_date) opened_date_month
    FROM base
    WHERE status IS NOT NULL
)
SELECT
    s.customer_id,
    s.account_type,
    s.opened_date,
    s.account_type_prefix,
    s.opened_date_month,
    CASE WHEN account_type RLIKE '^(CHECKING|CREDIT)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS account_type_zone,
    (SELECT MIN(t2.discount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS min_order_discount
FROM cleaned s
ORDER BY opened_date_month NULLS LAST;
