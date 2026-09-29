select ss.id, ss.revenue, ss.sale_date from store_sales ss where ss.product_id = 4 and ss.store_id not in (select id from stores where month(opened_date) in (2, 10, 12));
