select s.opened_date, s.store_name, ss.revenue, ss.product_id, ss.store_id from stores s inner join store_sales ss on ss.store_id = s.id order by revenue
