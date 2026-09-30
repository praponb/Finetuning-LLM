WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(opened_date) opened_date_month_end
    FROM accounts
    WHERE status != 'OPEN'
)
-- Multi-level aggregation
SELECT
    CASE WHEN balance >= 73000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    account_type,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) FILTER (WHERE status = 'FROZEN') AS frozen_rows
FROM valid
GROUP BY CASE WHEN balance >= 73000 THEN 'LARGE' ELSE 'SMALL' END, account_type
