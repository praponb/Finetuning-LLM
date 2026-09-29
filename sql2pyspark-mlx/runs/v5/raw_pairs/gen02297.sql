select date_trunc('MONTH', p.launch_date) launch_month, round(avg(ss.revenue), 2) avg_revenue
from store_sales ss
inner join products p on p.id = ss.product_id
inner join suppliers s on p.supplier_id = s.id
group by date_trunc('MONTH', p.launch_date)
order by avg_revenue desc
