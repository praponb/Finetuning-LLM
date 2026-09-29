with store_sales_by_product as (
    select product_id, count(distinct revenue) as unique_stores
    from store_sales
    where product_id = 1
    group by product_id
)
select
    products.id,
    products.product_name,
    products.supplier_id,
    store_sales_by_product.unique_stores
from products
join store_sales_by_product on products.id = store_sales_by_product.product_id
where store_sales_by_product.unique_stores >= 6
order by unique_stores desc
