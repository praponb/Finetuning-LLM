SELECT
    DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month,
    c.segment,
    SUM(CASE WHEN s.device = 'desktop' THEN 1 ELSE 0 END) AS desktop_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.browser))) AS browser_values
FROM sessions s
JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IS NOT NULL
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM'), c.segment;
