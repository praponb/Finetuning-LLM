SELECT orders.channel, stores.city, customers.email FROM orders JOIN stores ON orders.store_id = stores.id LEFT JOIN customers ON orders.customer_id = customers.id ORDER BY customers.email ASC;
