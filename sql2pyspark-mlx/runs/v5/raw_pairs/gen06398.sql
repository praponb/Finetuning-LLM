SELECT CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.method))) AS method_values, AVG(p.amount) AS avg_amount
FROM payments p
RIGHT JOIN orders o ON p.order_id = o.id
WHERE p.order_id IS NULL
GROUP BY CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END;
