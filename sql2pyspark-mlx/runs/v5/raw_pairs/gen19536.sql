SELECT status, MIN(balance) AS min_balance, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM accounts GROUP BY status
