SELECT orders.id, orders.discount, stores.store_name, stores.city, stores.sqft FROM orders LEFT JOIN stores ON orders.store_id = stores.id WHERE stores.sqft < 12032 ORDER BY stores.store_name;
