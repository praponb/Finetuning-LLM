SELECT QUARTER(s.opened_date) AS opened_quarter, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue, MAX(ss.units) AS max_units
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE p.brand IN ('Umbrella', 'Initech', 'Globex')
GROUP BY QUARTER(s.opened_date)
ORDER BY stddev_revenue
