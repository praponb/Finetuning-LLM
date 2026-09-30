select
    store_sales.id,
    store_sales.store_id,
    products.supplier_id,
    suppliers.city,
    ceil(products.price) as price_whole
from store_sales
inner join products on store_sales.product_id = products.id
left join suppliers on products.supplier_id = suppliers.id
where products.category in ('Grocery', 'Books')
