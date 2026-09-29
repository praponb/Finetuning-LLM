select *, lead(units, 1) over (partition by store_id order by revenue, id) as next_units, min(revenue) over (partition by product_id) as product_id_min_revenue from store_sales;
