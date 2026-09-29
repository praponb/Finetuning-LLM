SELECT CASE WHEN YEAR(delivered_date) < 2024 THEN 'before_2024' WHEN YEAR(delivered_date) = 2024 THEN '2024' ELSE 'after_2024' END AS delivered_cohort, carrier, SUM(cost) AS total_cost
FROM shipments
GROUP BY 1, 2;
