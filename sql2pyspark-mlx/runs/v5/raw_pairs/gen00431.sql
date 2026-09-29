WITH matched_keys AS (
    -- Set difference on keys
    SELECT customer_id, account_type
    FROM accounts
    EXCEPT ALL
    SELECT customer_id, account_type
    FROM accounts
    WHERE opened_date >= '2024-09-14'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.account_type = k.account_type
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    SUM(CASE WHEN account_type = 'CHECKING' THEN balance ELSE 0 END) AS checking_balance,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    GROUPING_ID() AS grouping_id
FROM verified
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', opened_date), DATE_TRUNC('MONTH', opened_date)), (DATE_TRUNC('WEEK', opened_date)))
HAVING checking_balance >= 0;
