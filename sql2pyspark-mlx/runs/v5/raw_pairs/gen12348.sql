SELECT MONTH(c.signup_date) AS signup_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.browser))) AS browser_values, SUM(CASE WHEN s.device = 'mobile' THEN 1 ELSE 0 END) AS mobile_count
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.customer_id IN (11, 14)
GROUP BY MONTH(c.signup_date)
ORDER BY mobile_count ASC;
