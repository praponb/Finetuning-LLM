select
    orders.id,
    orders.channel,
    orders.discount,
    customers.name,
    stores.sqft
from orders
join customers on orders.customer_id = customers.id
join stores on orders.store_id = stores.id
where lower(customers.name) like '%lic%';
