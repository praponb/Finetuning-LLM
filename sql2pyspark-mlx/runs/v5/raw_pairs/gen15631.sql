SELECT CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_count
FROM orders
WHERE shipping_fee BETWEEN 30 AND 33 AND discount <= 0.37
GROUP BY 1
HAVING SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END) > 62
ORDER BY order_cohort DESC
