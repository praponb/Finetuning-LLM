select
    orders.id,
    orders.order_date,
    orders.channel,
    stores.opened_date,
    customers.credit_limit,
    upper(trim(customers.email)) as email_normalized
from orders
inner join stores on orders.store_id = stores.id
full outer join customers on orders.customer_id = customers.id
order by email_normalized asc, id desc
