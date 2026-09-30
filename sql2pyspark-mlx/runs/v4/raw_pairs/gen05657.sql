WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC NULLS FIRST, id) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(opened_date) AS opened_date_month_end
    FROM ranked
    WHERE status IN ('CLOSED', 'OPEN')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.country,
        d.credit_limit,
        d.name
    FROM base src
    JOIN customers d
        ON src.customer_id = d.id
),
base_accounts AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        credit_limit + balance AS credit_limit_plus_balance,
        ADD_MONTHS(opened_date, 12) AS opened_date_plus_months
    FROM with_dims
    WHERE country IS NOT NULL
      AND balance >= 58000
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant = 'Grab'
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM kept
    EXCEPT ALL
    SELECT customer_id
    FROM accounts
    WHERE customer_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN matched_keys k ON s.customer_id = k.customer_id
)
-- Grouped metrics
SELECT
    CASE WHEN credit_limit_plus_balance >= 81 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_plus_balance_tier,
    DATE_TRUNC('WEEK', opened_date_month_end) AS week_start,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    AVG(credit_limit) FILTER (WHERE country = 'SG') AS sg_avg_credit_limit,
    MIN(credit_limit_plus_balance) AS min_credit_limit_plus_balance,
    GROUPING_ID() AS grouping_level
FROM verified
GROUP BY GROUPING SETS ((CASE WHEN credit_limit_plus_balance >= 81 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', opened_date_month_end)), (CASE WHEN credit_limit_plus_balance >= 81 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY credit_limit_plus_balance_tier
