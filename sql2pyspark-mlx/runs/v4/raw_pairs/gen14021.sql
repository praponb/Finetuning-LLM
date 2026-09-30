select
    stores.city,
    stores.opened_date,
    stores.store_name,
    orders.order_date,
    orders.customer_id,
    orders.channel
from stores
left join orders on stores.id = orders.store_id
where orders.order_date > '2024-11-01'
order by channel desc;
