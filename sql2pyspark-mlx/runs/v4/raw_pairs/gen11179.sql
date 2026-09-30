SELECT YEAR(s.opened_date) AS opened_year, AVG(o.amount) AS avg_amount
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
GROUP BY YEAR(s.opened_date);
