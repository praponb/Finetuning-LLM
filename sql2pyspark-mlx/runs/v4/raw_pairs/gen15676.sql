select
    *,
    ntile(3) over w_ordered as amount_bucket,
    corr(amount, shipping_fee) over (partition by channel, status) as corr_amount_shipping_fee
from orders
window w_ordered as (partition by store_id, channel order by amount desc, id)
