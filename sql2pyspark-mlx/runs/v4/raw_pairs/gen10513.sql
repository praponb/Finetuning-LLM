SELECT c.city, YEAR(c.signup_date) AS signup_year, COUNT(CASE WHEN s.browser = 'Firefox' THEN 1 END) AS firefox_count
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.city, YEAR(c.signup_date)
