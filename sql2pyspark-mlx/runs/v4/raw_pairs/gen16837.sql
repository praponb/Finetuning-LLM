select id, store_id, product_id, least(units, revenue) as least_units_revenue from store_sales where units is null and units between 18 and 33
