SELECT CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, SUM(p.amount) AS total_amount, SUM(CASE WHEN p.status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
LEFT JOIN customers c ON o.customer_id = c.id
GROUP BY CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY total_amount DESC;
