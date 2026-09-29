WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        balance,
        opened_date,
        status,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months
    FROM accounts
    WHERE account_type IN ('LOAN', 'CHECKING', 'CREDIT')
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date_plus_months
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.is_flagged
        )
),
cleaned_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN balance >= 64000 THEN 'High' WHEN balance >= 9000 THEN 'Medium' ELSE 'Low' END AS balance_band,
        DATE_SUB(opened_date, 55) AS opened_date_minus_55d,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM kept
    WHERE status = 'OPEN'
)
SELECT
    id,
    balance,
    opened_date,
    status,
    opened_date_plus_months,
    opened_date_minus_55d,
    CASE WHEN status RLIKE '^(FROZEN|OPEN)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS status_zone
FROM cleaned_accounts
