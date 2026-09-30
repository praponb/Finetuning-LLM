SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, COUNT(DISTINCT i.quantity) AS unique_stores
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
LEFT JOIN stores s ON i.store_id = s.id
GROUP BY DATE_TRUNC('MONTH', s.opened_date)
HAVING COUNT(DISTINCT i.quantity) > 2;
