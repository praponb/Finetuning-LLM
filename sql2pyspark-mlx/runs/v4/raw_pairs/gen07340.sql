with order_summary as (
    select order_id, min(unit_price) as min_unit_price
    from order_items
    where order_id in (18, 27)
    group by order_id
)
select
    o.id,
    o.shipping_fee,
    o.amount,
    os.min_unit_price
from orders o
join order_summary os on o.id = os.order_id
where os.min_unit_price > 170
order by shipping_fee desc, id desc
