SELECT
    MONTH(opened_date) opened_month,
    status,
    SUM(balance) total_balance,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) account_type_values
FROM accounts
GROUP BY MONTH(opened_date), status
ORDER BY status
