select ss.id, ss.revenue, ss.product_id, ss.store_id, p.category, p.launch_date from store_sales ss full outer join products p on ss.product_id = p.id where p.price >= 470
