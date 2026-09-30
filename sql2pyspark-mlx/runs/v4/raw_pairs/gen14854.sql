select ss.id, ss.sale_date, p.launch_date, p.category from store_sales ss left outer join products p on p.id = ss.product_id where p.price > 880;
