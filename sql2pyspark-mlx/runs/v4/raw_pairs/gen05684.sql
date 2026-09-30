SELECT
    s.id,
    s.city,
    s.country,
    t.cnt,
    t.first_launch_date
FROM suppliers s
JOIN (SELECT supplier_id, COUNT(*) AS cnt, MIN(launch_date) AS first_launch_date
    FROM products
    WHERE category NOT IN ('Home', 'Sports')
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY s.city;
