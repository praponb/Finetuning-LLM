SELECT s.region, CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort, MAX(i.quantity) max_quantity
FROM inventory i
FULL OUTER JOIN stores s ON i.store_id = s.id
GROUP BY s.region, CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
