select
    store_sales.revenue,
    store_sales.store_id,
    products.category,
    suppliers.supplier_name,
    lower(suppliers.city) as city_lower
from store_sales
left join products on store_sales.product_id = products.id
inner join suppliers on products.supplier_id = suppliers.id
where suppliers.country = 'US' and products.price is null
