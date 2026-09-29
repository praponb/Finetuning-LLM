select id, product_id, revenue, rank() over (partition by product_id order by revenue desc) as revenue_rank from store_sales where month(sale_date) in (4, 8, 11) order by 2 asc
