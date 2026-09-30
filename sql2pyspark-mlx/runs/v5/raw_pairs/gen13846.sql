select id, product_id, sale_date, units, lead(units, 1) over (partition by product_id order by sale_date, id) as next_units from store_sales;
