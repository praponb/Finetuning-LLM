select store_id, product_id from store_sales intersect select store_id, product_id from store_sales where units between 19 and 31;
