SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    customer_id,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    MAX(balance) FILTER (WHERE status = 'CLOSED') AS closed_max_balance,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END), 0), 2) AS balance_per_checking,
    COUNT(*) AS row_count,
    GROUPING(DATE_TRUNC('YEAR', opened_date)) AS gid
FROM accounts
GROUP BY ROLLUP (DATE_TRUNC('YEAR', opened_date), customer_id, DATE_TRUNC('QUARTER', opened_date))
ORDER BY year_start ASC;
