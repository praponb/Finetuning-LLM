with product_summary as (
    select product_id, count(*) as total_count, count(distinct unit_price) as unique_products
    from order_items
    group by product_id
)
select p.cost, ps.total_count, ps.unique_products
from products p
inner join product_summary ps on p.id = ps.product_id
order by total_count desc, unique_products desc;
