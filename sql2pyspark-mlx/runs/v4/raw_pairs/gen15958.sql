select suppliers.country, count(store_sales.revenue) revenue_count, count(distinct store_sales.revenue) unique_stores
from store_sales
inner join products on store_sales.product_id = products.id
inner join suppliers on suppliers.id = products.supplier_id
group by suppliers.country
order by country asc, revenue_count desc;
