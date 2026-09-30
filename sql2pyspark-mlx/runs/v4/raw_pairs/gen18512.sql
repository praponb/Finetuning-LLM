SELECT
    orders.discount,
    stores.sqft,
    stores.store_name,
    customers.city,
    customers.country
FROM orders
INNER JOIN stores ON stores.id = orders.store_id
INNER JOIN customers ON customers.id = orders.customer_id
WHERE customers.city IN ('London', 'Tokyo') AND customers.age BETWEEN 46 AND 65
ORDER BY customers.country DESC;
