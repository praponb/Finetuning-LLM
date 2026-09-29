SELECT CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END AS shipped_cohort, MIN(cost) AS min_cost
FROM shipments
GROUP BY CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) < 4;
