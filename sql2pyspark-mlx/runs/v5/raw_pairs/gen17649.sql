select
    store_sales.id,
    store_sales.sale_date,
    store_sales.units,
    products.cost,
    products.category,
    stores.region,
    stores.store_name
from store_sales
left outer join products on store_sales.product_id = products.id
join stores on stores.id = store_sales.store_id
order by products.cost desc, id
limit 5;
