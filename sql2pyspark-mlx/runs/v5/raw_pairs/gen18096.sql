SELECT i.quantity, p.product_name, p.cost, s.city FROM inventory i INNER JOIN products p ON i.product_id = p.id FULL OUTER JOIN stores s ON i.store_id = s.id WHERE UPPER(s.store_name) LIKE '%IVE%';
