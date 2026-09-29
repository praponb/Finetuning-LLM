select product_id, id from store_sales except select product_id, id from store_sales where revenue <= 3300;
