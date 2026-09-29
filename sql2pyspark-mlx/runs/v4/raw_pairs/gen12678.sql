SELECT CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, QUARTER(s.opened_date) AS opened_quarter, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
JOIN stores s ON s.id = i.store_id
JOIN products p ON i.product_id = p.id
GROUP BY CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END, QUARTER(s.opened_date);
