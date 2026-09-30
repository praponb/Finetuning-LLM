SELECT a.id, a.store_id, a.units, a.revenue, p.store_name, p.city, p.opened_date FROM store_sales a JOIN stores p ON a.store_id = p.id WHERE p.city NOT IN ('Bangkok', 'Chiang Mai', 'New York')
