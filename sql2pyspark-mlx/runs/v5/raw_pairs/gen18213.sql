select id, store_id, product_id, sale_date, units, units - lag(units, 1) over (partition by store_id, product_id order by sale_date, id) units_change from store_sales where units <> 28;
