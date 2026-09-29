select id, product_id from store_sales where store_id in (2, 3, 4, 6) and (product_id = 3 or revenue < 2100) order by id desc
