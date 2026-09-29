select ss.store_id, ss.sale_date, ss.revenue, s.store_name, s.region, s.opened_date from store_sales ss right outer join stores s on s.id = ss.store_id
