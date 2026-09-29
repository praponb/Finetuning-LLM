with base as (
    select
        id,
        customer_id,
        store_id,
        channel,
        amount,
        discount,
        shipping_fee,
        concat_ws('/', channel, status) as channel_status_key
    from orders
    where order_date > date '2023-10-04'
      and channel is not null
)
select
    id,
    customer_id,
    channel,
    shipping_fee,
    channel_status_key,
    lag(discount) over w_grp as prev_discount,
    lead(shipping_fee) over w_grp as next_shipping_fee,
    row_number() over w_grp as seq_no,
    last_value(shipping_fee) ignore nulls over (partition by channel, store_id order by discount, id rows between unbounded preceding and current row) as last_known_shipping_fee
from base
window w_grp as (partition by channel order by amount, id)
