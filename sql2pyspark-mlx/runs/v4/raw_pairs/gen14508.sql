SELECT a.*, s.opened_date, s.city FROM store_sales a LEFT JOIN stores s ON a.store_id = s.id
