SELECT
    s.id,
    s.city,
    s.rating,
    t.avg_cost,
    t.stddev_cost
FROM suppliers s
INNER JOIN (SELECT supplier_id, ROUND(AVG(cost), 2) AS avg_cost, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM products
    WHERE category = 'Electronics'
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.stddev_cost >= 35
ORDER BY rating DESC, id DESC
LIMIT 1;
