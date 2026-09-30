SELECT account_type, avg_balance, status_list
FROM (
    SELECT account_type, AVG(balance) AS avg_balance, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM accounts
    GROUP BY account_type
) AS t
WHERE avg_balance < 52000;
