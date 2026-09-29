SELECT QUARTER(p.launch_date) AS launch_quarter, AVG(ss.units) AS avg_units
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON ss.store_id = s.id
WHERE s.opened_date <= '2025-04-27'
GROUP BY QUARTER(p.launch_date)
HAVING AVG(ss.units) >= 22
ORDER BY avg_units DESC;
