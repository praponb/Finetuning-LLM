with cleaned as (
    -- Keep recent, valid rows
    select *, case when channel in ('mobile', 'web') then 'Tier 1' else 'Other' end channel_group
    from orders
    where customer_id is not null
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(shipping_fee, 0) shipping_fee_filled,
        discount + amount discount_plus_amount,
        coalesce(channel, 'N/A') channel_filled
    from cleaned
    where order_date >= date '2023-05-16'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_recent shipping_fee_cume_dist,
        last_value(channel, true) over (partition by store_id order by order_date, id rows between unbounded preceding and current row) last_known_channel,
        cume_dist() over w_part shipping_fee_cume_dist_2,
        row_number() over (order by shipping_fee_filled, id) event_seq,
        round(amount / sum(amount) over (partition by channel), 4) amount_share
    from filtered
    window w_recent as (partition by store_id, channel order by shipping_fee), w_part as (order by shipping_fee desc)
)
select
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.channel_group,
    s.shipping_fee_filled,
    s.discount_plus_amount,
    (select max(t2.shipping_fee) from orders t2 where t2.customer_id = s.customer_id) as max_order_shipping_fee
from metrics s
order by id desc, channel
