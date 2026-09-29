select id, revenue, units + revenue as revenue_per_unit from store_sales where units between 29 and 36 order by revenue desc, revenue_per_unit desc, id limit 5;
