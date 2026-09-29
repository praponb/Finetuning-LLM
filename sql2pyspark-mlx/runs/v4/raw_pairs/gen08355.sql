SELECT DATE_TRUNC('MONTH', c.signup_date) AS signup_month, COUNT(*) AS num_sessions, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.browser))) AS browser_values
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY DATE_TRUNC('MONTH', c.signup_date)
ORDER BY 2;
