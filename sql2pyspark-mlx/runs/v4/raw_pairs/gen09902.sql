SELECT CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, AVG(discount) AS avg_discount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM orders
WHERE order_date <= '2024-01-01'
GROUP BY 1
HAVING avg_discount < 0.16
