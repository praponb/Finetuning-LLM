select p.id, p.launch_date from products p where not exists (select 1 from store_sales ss where ss.product_id = p.id);
