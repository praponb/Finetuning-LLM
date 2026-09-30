select id, product_id, sale_date, units, units - lag(units, 1) over (partition by product_id order by sale_date, id) as units_change from store_sales
