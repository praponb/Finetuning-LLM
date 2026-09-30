select
    customer_id,
    store_id,
    concat_ws(', ', sort_array(collect_set(channel))) as channel_values,
    sort_array(collect_set(status)) as status_list
from orders
where customer_id in (4, 35) and channel in ('partner', 'store') and order_date < '2023-03-01'
group by customer_id, store_id
