WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        ADD_MONTHS(opened_date, 6) AS opened_date_plus_months
    FROM accounts
    WHERE opened_date >= DATE '2023-03-28'
      AND customer_id IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date_plus_months
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        balance,
        opened_date,
        status,
        opened_date_plus_months,
        CASE WHEN status IN ('OPEN', 'CLOSED') THEN 'Core' ELSE 'Other' END AS status_group
    FROM not_flagged
    WHERE opened_date >= TO_DATE('2024-03-20')
      AND customer_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM base
    EXCEPT ALL
    SELECT status, id
    FROM accounts
    WHERE opened_date < '2025-05-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
totals AS (
    -- Grouped metrics
    SELECT
        status,
        DATE_TRUNC('MONTH', opened_date_plus_months) AS activity_month,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        SIZE(COLLECT_SET(status)) AS num_status,
        MAX(balance) AS max_balance,
        COUNT(*) AS row_count,
        SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance,
        APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
    FROM reconciled
    GROUP BY GROUPING SETS ((status, DATE_TRUNC('MONTH', opened_date_plus_months)), (status), ())
)
SELECT
    status,
    activity_month,
    num_status,
    max_balance,
    row_count,
    CASE WHEN status RLIKE '^(CLOSED|OPEN)$' THEN 'PRIORITY' WHEN status RLIKE '^(FROZEN)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS status_zone
FROM totals;
