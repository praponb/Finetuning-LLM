select store_id, avg(revenue) as avg_revenue, min(revenue) as min_revenue from store_sales group by store_id having min(revenue) >= 2100
