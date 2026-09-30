SELECT CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, status, COUNT(*) AS num_payments
FROM payments
WHERE amount BETWEEN 640 AND 740 OR method = 'cash'
GROUP BY 1, 2
ORDER BY 3 DESC;
