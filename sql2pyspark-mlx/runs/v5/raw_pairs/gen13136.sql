SELECT CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END paid_cohort, SUM(amount) total_amount
FROM payments
WHERE amount <= 1410 AND method = 'card'
GROUP BY paid_cohort
HAVING SUM(amount) < 5600;
