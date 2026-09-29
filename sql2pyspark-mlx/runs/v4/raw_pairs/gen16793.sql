SELECT s.country, t.latest_launch_date, t.unique_categorys
FROM suppliers s
INNER JOIN (SELECT supplier_id, MAX(launch_date) AS latest_launch_date, COUNT(DISTINCT cost) AS unique_categorys
    FROM products
    WHERE cost IS NULL
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.unique_categorys > 7
