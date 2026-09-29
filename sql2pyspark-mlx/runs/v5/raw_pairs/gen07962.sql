SELECT CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END AS shipped_cohort, SUM(cost) AS total_cost, DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank
FROM shipments
WHERE status = 'LOST'
GROUP BY CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END;
