select
    store_sales.id,
    store_sales.product_id,
    store_sales.revenue,
    products.stock_qty,
    products.category,
    suppliers.rating,
    suppliers.city
from store_sales
join products on products.id = store_sales.product_id
right outer join suppliers on products.supplier_id = suppliers.id
order by suppliers.rating
