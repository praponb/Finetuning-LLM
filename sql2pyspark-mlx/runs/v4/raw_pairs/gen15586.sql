select
    stores.city,
    stores.region,
    round(avg(store_sales.units), 2) as avg_units,
    count(store_sales.units) as units_count
from store_sales
join stores on store_sales.store_id = stores.id
left join products on store_sales.product_id = products.id
group by stores.city, stores.region
