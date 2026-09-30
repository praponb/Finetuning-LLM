select
    *,
    ntile(4) over w_hist as amount_bucket,
    stddev_samp(shipping_fee) over w_hist_2 as status_stddev_samp_shipping_fee,
    sum(shipping_fee) over (partition by status order by discount, id rows between unbounded preceding and current row) as running_shipping_fee
from orders
window w_hist as (partition by store_id order by amount, id), w_hist_2 as (partition by status, store_id)
