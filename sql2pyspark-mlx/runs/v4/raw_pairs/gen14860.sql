SELECT YEAR(s.opened_date) AS opened_year, s.region, AVG(o.amount) AS avg_amount
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE o.customer_id IS NULL
GROUP BY YEAR(s.opened_date), s.region
ORDER BY s.region DESC;
