with product_totals as (
    select product_id, count(distinct quantity) unique_products
    from inventory
    where product_id in (5, 8, 9)
    group by product_id
)
select
    products.id,
    products.product_name,
    products.supplier_id,
    product_totals.unique_products
from products
left outer join product_totals on products.id = product_totals.product_id
order by product_totals.unique_products
