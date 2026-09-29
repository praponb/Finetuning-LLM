select
    payments.id,
    payments.method,
    payments.status,
    payments.paid_at,
    orders.status as order_status,
    orders.customer_id,
    orders.shipping_fee,
    lower(payments.status) as status_lower
from payments
inner join orders on payments.order_id = orders.id
