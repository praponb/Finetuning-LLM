SELECT QUARTER(s.opened_date) AS opened_quarter, s.city, SUM(CASE WHEN o.status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE c.credit_limit IS NOT NULL
GROUP BY QUARTER(s.opened_date), s.city
ORDER BY cancelled_count, opened_quarter, city
LIMIT 50;
