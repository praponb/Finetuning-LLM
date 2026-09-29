SELECT suppliers.id, suppliers.rating, t.first_launch_date
FROM suppliers
LEFT JOIN (SELECT supplier_id, MIN(launch_date) AS first_launch_date
    FROM products
    GROUP BY supplier_id) t ON suppliers.id = t.supplier_id
