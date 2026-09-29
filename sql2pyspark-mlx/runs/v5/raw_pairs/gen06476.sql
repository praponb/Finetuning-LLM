WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN balance >= 64000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    AVG(balance) FILTER (WHERE status = 'CLOSED') AS closed_avg_balance,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count,
    GROUPING(DATE_TRUNC('WEEK', opened_date)) AS grouping_id
FROM ranked
GROUP BY CUBE (DATE_TRUNC('WEEK', opened_date), DATE_TRUNC('QUARTER', opened_date), CASE WHEN balance >= 64000 THEN 'HIGH' ELSE 'LOW' END)
HAVING closed_avg_balance > 0;
