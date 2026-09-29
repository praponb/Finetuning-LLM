with order_totals as (
    select order_id, sum(case when method = 'paypal' then 1 else 0 end) as paypal_count
    from payments
    where method = 'cash'
    group by order_id
)
select
    o.id,
    o.amount,
    o.store_id,
    ot.paypal_count
from orders o
inner join order_totals ot on o.id = ot.order_id
where ot.paypal_count >= 35
order by amount, id
limit 5;
