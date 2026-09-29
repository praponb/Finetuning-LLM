SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    customer_id,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    AVG(balance) FILTER (WHERE account_type = 'SAVINGS') AS savings_avg_balance,
    STDDEV_SAMP(balance) AS balance_stddev,
    GROUPING_ID() AS grouping_level
FROM accounts
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', opened_date), DATE_TRUNC('QUARTER', opened_date), customer_id), (DATE_TRUNC('YEAR', opened_date), DATE_TRUNC('QUARTER', opened_date)))
ORDER BY quarter_start NULLS FIRST;
