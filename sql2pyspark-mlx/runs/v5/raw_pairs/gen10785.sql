SELECT status, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, ROUND((100 * SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_card
FROM payments
WHERE status != 'REFUNDED'
GROUP BY status, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY status DESC
