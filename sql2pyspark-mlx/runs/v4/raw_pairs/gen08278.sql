SELECT DATE_TRUNC('MONTH', c.signup_date) AS signup_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.browser))) AS browser_values
FROM sessions s
JOIN customers c ON s.customer_id = c.id
GROUP BY DATE_TRUNC('MONTH', c.signup_date)
