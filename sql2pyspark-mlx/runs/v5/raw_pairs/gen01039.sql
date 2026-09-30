with OrderItemsByProduct as (
    select product_id, count(distinct quantity) as unique_products
    from order_items
    where unit_price >= 160
    group by product_id
)
select
    p.id,
    p.cost,
    p.price,
    O.unique_products
from products p
inner join OrderItemsByProduct O on p.id = O.product_id
where O.unique_products > 4
order by p.price desc, p.cost desc
