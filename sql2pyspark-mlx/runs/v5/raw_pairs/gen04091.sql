SELECT customer_id, CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END end_cohort, ROUND(AVG(monthly_fee), 2) avg_monthly_fee
FROM subscriptions
GROUP BY customer_id, CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING avg_monthly_fee > 54;
