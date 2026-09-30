SELECT
    status,
    CASE WHEN status IN ('FROZEN', 'CLOSED') THEN 'core' ELSE 'other' END AS status_group,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values,
    ROUND(STDDEV(balance), 2) AS stddev_balance
FROM accounts
WHERE status IN ('FROZEN', 'OPEN', 'CLOSED') AND account_type = 'CREDIT'
GROUP BY status, CASE WHEN status IN ('FROZEN', 'CLOSED') THEN 'core' ELSE 'other' END
HAVING ROUND(STDDEV(balance), 2) > 40
