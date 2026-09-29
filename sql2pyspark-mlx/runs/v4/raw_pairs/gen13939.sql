SELECT
    s.opened_date,
    s.city,
    t.stddev_units,
    t.avg_units
FROM stores s
JOIN (SELECT store_id, ROUND(STDDEV(units), 2) AS stddev_units, ROUND(AVG(units), 2) AS avg_units
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.avg_units >= 43
ORDER BY city
