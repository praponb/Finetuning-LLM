select
    products.brand,
    products.product_name,
    products.supplier_id,
    store_sales.revenue,
    store_sales.product_id
from products
left outer join store_sales on products.id = store_sales.product_id
where array_contains(products.tags, 'premium');
