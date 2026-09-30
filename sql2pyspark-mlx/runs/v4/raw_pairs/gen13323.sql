SELECT
    CASE WHEN balance >= 84000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    ROUND(AVG(balance), 2) AS avg_balance,
    COUNT_IF(status = 'FROZEN') AS frozen_events,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list_2,
    SUM(balance) AS total_balance
FROM accounts
GROUP BY balance_tier;
