SELECT QUARTER(c.signup_date) AS signup_quarter, COUNT(we.duration_sec) AS duration_sec_count
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
JOIN customers c ON c.id = s.customer_id
WHERE c.age = 47
GROUP BY QUARTER(c.signup_date)
HAVING COUNT(we.duration_sec) >= 1
ORDER BY duration_sec_count, 1 DESC
