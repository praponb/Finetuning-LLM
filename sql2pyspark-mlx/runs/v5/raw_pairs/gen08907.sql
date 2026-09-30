SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, ROUND(STDDEV(ss.units), 2) AS stddev_units, SUM(ss.units) AS total_units
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
JOIN stores s ON s.id = ss.store_id
WHERE s.region NOT IN ('Central', 'South', 'West')
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
HAVING stddev_units >= 52
ORDER BY total_units;
