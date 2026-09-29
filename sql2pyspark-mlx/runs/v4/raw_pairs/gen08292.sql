SELECT
    c.segment,
    DATE_TRUNC('MONTH', c.signup_date) AS signup_month,
    COUNT(DISTINCT we.duration_sec) AS unique_pages,
    ROUND(STDDEV(we.duration_sec), 2) AS stddev_duration_sec
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
FULL OUTER JOIN customers c ON c.id = s.customer_id
WHERE s.device = 'mobile'
GROUP BY c.segment, DATE_TRUNC('MONTH', c.signup_date)
ORDER BY signup_month, segment
LIMIT 10;
