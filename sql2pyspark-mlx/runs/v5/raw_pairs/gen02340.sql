SELECT CASE WHEN YEAR(delivered_date) < 2024 THEN 'before_2024' WHEN YEAR(delivered_date) = 2024 THEN '2024' ELSE 'after_2024' END AS delivered_cohort, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY CASE WHEN YEAR(delivered_date) < 2024 THEN 'before_2024' WHEN YEAR(delivered_date) = 2024 THEN '2024' ELSE 'after_2024' END) AS cumulative_cost
FROM shipments
WHERE MONTH(delivered_date) IN (1, 4, 9)
GROUP BY CASE WHEN YEAR(delivered_date) < 2024 THEN 'before_2024' WHEN YEAR(delivered_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY cumulative_cost
