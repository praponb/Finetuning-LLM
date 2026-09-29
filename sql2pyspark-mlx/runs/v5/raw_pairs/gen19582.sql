select store_id, unique_products from (select store_id, count(distinct revenue) unique_products from store_sales group by store_id) t where unique_products > 5
