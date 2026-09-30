select id, store_id, sale_date, revenue, greatest(units, revenue) greatest_units_revenue from store_sales
