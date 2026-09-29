select s.id, s.store_id, s.product_id, s.units, s.revenue, p2.opened_date, p2.city from store_sales s join stores p2 on s.store_id = p2.id where p2.store_name like 'M%'
