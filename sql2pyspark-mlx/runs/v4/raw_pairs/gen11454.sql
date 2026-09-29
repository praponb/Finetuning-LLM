SELECT CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, MIN(s.started_at) AS first_started_at
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.country IS NOT NULL
GROUP BY CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
