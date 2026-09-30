SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    COUNT_IF(status = 'OPEN') AS open_events,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SUM(CASE WHEN account_type = 'CHECKING' THEN balance ELSE 0 END) AS checking_balance
FROM accounts
GROUP BY CUBE (DATE_TRUNC('QUARTER', opened_date));
