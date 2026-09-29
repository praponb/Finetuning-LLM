SELECT CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, MIN(s.monthly_fee) AS min_monthly_fee, ROUND(AVG(s.monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.city = 'Chiang Mai'
GROUP BY CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END;
