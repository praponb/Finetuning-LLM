with with_windows as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        corr(quantity, unit_price) over (partition by product_id, order_id) as corr_quantity_unit_price,
        round(quantity / sum(quantity) over (partition by product_id), 4) as quantity_share,
        round(unit_price / sum(unit_price) over (partition by order_id), 4) as unit_price_share
    from order_items
),
with_dims as (
    -- Attach orders attributes
    select
        s.*,
        d.channel,
        d.shipping_fee,
        d.status
    from with_windows s
    left join orders d
        on s.order_id = d.id
),
scored as (
    -- Running and moving aggregates
    select
        *,
        sum(quantity_share) over (partition by status order by quantity_share, id rows between unbounded preceding and current row) as cumulative_quantity_share,
        stddev_samp(unit_price) over (partition by channel) as channel_stddev_samp_unit_price,
        lead(quantity, 1) over (partition by channel order by unit_price, id) as next_quantity,
        sum(corr_quantity_unit_price) over (partition by order_id order by shipping_fee, id rows between unbounded preceding and current row) as cumulative_corr_quantity_unit_price
    from with_dims
),
with_windows_order_items as (
    -- Window metrics over several different partitions
    select
        *,
        corr(unit_price, quantity) over (partition by order_id) as corr_unit_price_quantity,
        last_value(quantity_share) ignore nulls over (partition by order_id, product_id order by unit_price, id rows between unbounded preceding and current row) as last_known_quantity_share
    from scored
),
daily as (
    -- Aggregate with conditional metrics
    select
        channel,
        order_id,
        round(sum(shipping_fee) / nullif(count(case when status = 'COMPLETED' then 1 end), 0), 2) as shipping_fee_per_completed,
        count(*) filter (where channel = 'store') as store_rows,
        count(*) as event_count,
        avg(quantity) filter (where status = 'PENDING') as pending_avg_quantity,
        count_if(channel = 'mobile') as mobile_events,
        approx_percentile(quantity, 0.9) as quantity_p90
    from with_windows_order_items
    group by channel, order_id
    having pending_avg_quantity < 0
)
select
    channel,
    order_id,
    shipping_fee_per_completed,
    store_rows,
    quantity_p90,
    case when channel rlike '^(web|store)$' then 'OTHER_ZONE' when channel rlike '^(mobile|partner)$' then 'CORE' else 'GROUP_B' end as channel_zone
from daily
order by channel desc nulls first, shipping_fee_per_completed
