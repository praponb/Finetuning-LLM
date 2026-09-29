select
    orders.status,
    orders.customer_id,
    count(distinct payments.amount) as unique_methods,
    min(payments.amount) as min_amount
from payments
left outer join orders on payments.order_id = orders.id
inner join stores on orders.store_id = stores.id
where orders.channel in ('partner', 'mobile')
group by orders.status, orders.customer_id
order by status desc
