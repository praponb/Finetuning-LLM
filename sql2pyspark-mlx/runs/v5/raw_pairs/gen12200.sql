SELECT
    QUARTER(opened_date) AS opened_quarter,
    status,
    account_type,
    ROUND(STDDEV(balance), 2) AS stddev_balance,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
FROM accounts
GROUP BY opened_quarter, status, account_type;
