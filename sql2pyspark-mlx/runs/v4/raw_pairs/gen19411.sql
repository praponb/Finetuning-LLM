with product_stats as (
    select product_id, round(avg(quantity), 2) as avg_quantity
    from inventory
    group by product_id
)
select p.price, p.product_name, ps.avg_quantity
from products p
inner join product_stats ps on p.id = ps.product_id
where ps.avg_quantity >= 33
order by product_name asc
