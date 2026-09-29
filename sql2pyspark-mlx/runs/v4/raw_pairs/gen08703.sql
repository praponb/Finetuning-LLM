with product_totals as (
    select product_id, max(quantity) - min(quantity) as quantity_range
    from inventory
    where quantity between 218 and 223
    group by product_id
)
select
    p.id,
    p.price,
    p.product_name,
    pt.quantity_range
from products p
inner join product_totals pt on p.id = pt.product_id
where pt.quantity_range > 61;
