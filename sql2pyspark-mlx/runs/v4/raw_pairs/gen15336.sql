SELECT YEAR(s.opened_date) opened_year, MAX(o.amount) - MIN(o.amount) amount_range
FROM orders o
JOIN stores s ON s.id = o.store_id
JOIN customers c ON o.customer_id = c.id
WHERE s.store_name IS NOT NULL
GROUP BY YEAR(s.opened_date)
HAVING MAX(o.amount) - MIN(o.amount) > 36
ORDER BY 1 DESC
