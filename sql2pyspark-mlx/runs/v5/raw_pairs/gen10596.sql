select stores.region, avg(store_sales.units) as avg_units
from store_sales
join products on store_sales.product_id = products.id
join stores on store_sales.store_id = stores.id
group by stores.region
order by avg_units desc, region
limit 50
