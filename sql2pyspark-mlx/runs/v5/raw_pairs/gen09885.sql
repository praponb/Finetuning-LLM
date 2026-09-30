select s.id, s.opened_date from stores s where s.region = 'Central' and s.id in (select store_id from store_sales where product_id in (1, 4, 9))
