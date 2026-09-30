select s.city, s.opened_date, s.store_name, ss.revenue, ss.sale_date, ss.product_id, coalesce(s.sqft, 0) sqft_filled from stores s left join store_sales ss on ss.store_id = s.id;
