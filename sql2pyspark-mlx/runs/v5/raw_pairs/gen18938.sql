select id, sale_date, revenue, sum(revenue) over (order by sale_date, id) as running_revenue from store_sales
