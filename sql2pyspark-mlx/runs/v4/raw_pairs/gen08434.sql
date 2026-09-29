with product_summary as (
    select product_id, count(quantity) as quantity_count, min(unit_price) as min_unit_price
    from order_items
    group by product_id
)
select
    p.product_name,
    p.supplier_id,
    ps.quantity_count,
    ps.min_unit_price
from products p
inner join product_summary ps on p.id = ps.product_id
order by p.product_name;
