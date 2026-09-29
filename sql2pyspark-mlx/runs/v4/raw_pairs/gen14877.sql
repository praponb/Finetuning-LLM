select count(revenue) revenue_count, max(revenue) max_revenue from store_sales where revenue is not null and revenue is not null order by revenue_count desc
