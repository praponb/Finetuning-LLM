select p.category, round(avg(ss.revenue), 2) as avg_revenue
from store_sales ss
inner join products p on ss.product_id = p.id
group by p.category
order by category;
