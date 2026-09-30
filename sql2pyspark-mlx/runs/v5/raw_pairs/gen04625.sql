WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant IS NOT NULL
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        CASE WHEN status IN ('OPEN', 'FROZEN') THEN 'Core' ELSE 'Other' END status_group,
        COALESCE(status, 'OTHER') resolved_status
    FROM eligible
    WHERE opened_date >= DATE '2023-06-08'
      AND account_type IN ('CHECKING', 'CREDIT', 'SAVINGS')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type, customer_id ORDER BY opened_date DESC, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC NULLS FIRST, id) = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(opened_date) opened_date_month_end
    FROM latest
    WHERE customer_id IS NOT NULL
      AND customer_id IS NULL
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', opened_date_month_end) AS activity_month,
    CASE WHEN balance >= 68000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END), 0), 2) AS balance_per_credit,
    STDDEV_SAMP(balance) AS balance_stddev,
    COUNT(*) AS cnt,
    GROUPING_ID() AS grouping_id
FROM valid
GROUP BY CUBE (DATE_TRUNC('MONTH', opened_date_month_end), CASE WHEN balance >= 68000 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING balance_stddev < 1000000
ORDER BY balance_stddev
