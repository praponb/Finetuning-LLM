select date_format(products.launch_date, 'yyyy-MM') as launch_month, max(store_sales.revenue) as max_revenue, round(avg(store_sales.revenue), 2) as avg_revenue
from store_sales
inner join products on store_sales.product_id = products.id
right outer join stores on store_sales.store_id = stores.id
where store_sales.store_id = 2
group by date_format(products.launch_date, 'yyyy-MM')
having round(avg(store_sales.revenue), 2) > 45;
