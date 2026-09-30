SELECT s.region, ROUND(STDDEV(ss.units), 2) AS stddev_units
FROM store_sales ss
LEFT OUTER JOIN stores s ON s.id = ss.store_id
GROUP BY s.region
HAVING stddev_units > 24
ORDER BY stddev_units DESC, region
