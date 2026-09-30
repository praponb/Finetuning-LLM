with joined as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.method,
        o.shipping_fee,
        o.amount as order_amount,
        o.channel
    from payments src
    left join orders o
        on src.order_id = o.id
),
with_dims as (
    -- Attach orders attributes
    select
        a.*,
        d.channel as order_channel,
        d.discount
    from joined a
    left join orders d
        on a.order_id = d.id
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        concat(channel, '/', method) as channel_method,
        length(method) as method_length
    from with_dims
    where order_amount >= 1420
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        shipping_fee,
        order_amount,
        discount,
        count(discount) over (partition by channel, order_id order by order_amount, id rows between unbounded preceding and current row) as running_count_discount,
        min(discount) over (partition by channel, order_id order by order_amount, id rows between unbounded preceding and current row) as running_min_discount,
        max(discount) over (partition by channel, order_id order by order_amount, id rows between unbounded preceding and current row) as running_max_discount,
        avg(shipping_fee) over (partition by order_channel order by discount, id rows between 3 preceding and current row) as shipping_fee_avg_3
    from cleaned
    where channel is null
)
-- Aggregate with conditional metrics
select
    case when shipping_fee_avg_3 >= 40 then 'LARGE' else 'SMALL' end as shipping_fee_avg_3_tier,
    sum(running_max_discount) as total_running_max_discount,
    round(avg(shipping_fee), 2) as avg_shipping_fee,
    count(*) as cnt
from with_windows
group by shipping_fee_avg_3_tier
order by total_running_max_discount desc nulls last
