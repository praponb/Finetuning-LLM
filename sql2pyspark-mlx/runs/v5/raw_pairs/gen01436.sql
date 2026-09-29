SELECT i.last_restock, s.city, p.category, p.brand FROM inventory i JOIN stores s ON s.id = i.store_id LEFT OUTER JOIN products p ON i.product_id = p.id
