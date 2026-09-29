select *, ntile(10) over (partition by product_id order by revenue, id) as revenue_bucket, ntile(3) over (order by units, id) as units_bucket from store_sales
