SELECT CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, SUM(amount) AS total_amount
FROM payments
WHERE method <> 'promptpay' AND amount >= 910
GROUP BY 1
ORDER BY paid_cohort;
