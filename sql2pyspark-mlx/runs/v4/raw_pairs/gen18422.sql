select
    id,
    store_id,
    shipping_fee,
    max(shipping_fee) over (partition by store_id) as store_id_max_shipping_fee
from orders
order by store_id_max_shipping_fee desc;
