with order_stats as (
    select order_id, count(amount) as amount_count
    from payments
    group by order_id
)
select o.discount, os.amount_count
from orders o
left join order_stats os on o.id = os.order_id
where os.amount_count >= 6
order by amount_count desc, o.discount desc
