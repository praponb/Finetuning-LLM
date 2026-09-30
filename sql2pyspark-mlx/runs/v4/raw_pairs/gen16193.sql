select *, cume_dist() over (partition by store_id order by revenue desc nulls last) as revenue_cume_dist, ntile(3) over (order by revenue desc, id) as revenue_bucket from store_sales
