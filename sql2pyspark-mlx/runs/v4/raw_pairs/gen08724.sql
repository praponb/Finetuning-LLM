SELECT orders.shipping_fee, stores.store_name, customers.age, customers.city FROM orders LEFT JOIN stores ON stores.id = orders.store_id LEFT JOIN customers ON orders.customer_id = customers.id;
