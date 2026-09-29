select product_id, sale_date from store_sales where store_id in (1, 4, 5, 6) union all select product_id, sale_date from store_sales where store_id in (3, 5, 6) order by product_id desc;
