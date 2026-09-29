SELECT s.opened_date, i.store_id FROM stores s INNER JOIN inventory i ON s.id = i.store_id;
