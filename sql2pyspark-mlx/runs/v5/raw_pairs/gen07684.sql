select
    payments.id,
    payments.status,
    payments.amount,
    orders.order_date
from payments
join orders on payments.order_id = orders.id
order by orders.order_date, payments.status, id
limit 3;
