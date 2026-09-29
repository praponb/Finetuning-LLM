SELECT CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.status))) AS status_values
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
LEFT JOIN stores s ON s.id = o.store_id
GROUP BY CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY opened_cohort DESC
LIMIT 20
