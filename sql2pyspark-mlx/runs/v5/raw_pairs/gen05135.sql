SELECT s.city, DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, SUM(CASE WHEN o.channel = 'partner' THEN 1 ELSE 0 END) AS partner_count
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
WHERE o.channel IN ('partner', 'mobile')
GROUP BY s.city, DATE_FORMAT(s.opened_date, 'yyyy-MM')
ORDER BY city, partner_count
