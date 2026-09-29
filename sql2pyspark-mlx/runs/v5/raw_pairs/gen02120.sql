SELECT
    orders.id,
    orders.status,
    stores.region,
    stores.sqft,
    customers.credit_limit
FROM orders
LEFT JOIN stores ON stores.id = orders.store_id
FULL OUTER JOIN customers ON orders.customer_id = customers.id
WHERE stores.sqft BETWEEN 6235 AND 7837
ORDER BY region DESC;
