with enriched as (
    -- Attach orders attributes
    select
        src.*,
        d.shipping_fee,
        d.status as order_status
    from shipments src
    left join orders d
        on src.order_id = d.id
),
with_dims as (
    -- Attach orders attributes
    select
        a.*,
        o.order_date,
        o.amount
    from enriched a
    join orders o
        on a.order_id = o.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        max(amount) over (partition by order_id) as order_id_max_amount,
        avg(amount) over (order by amount, id rows between 2 preceding and current row) as amount_avg_2,
        first_value(amount, true) over (order by cost, id rows between unbounded preceding and current row) as first_known_amount,
        ntile(10) over (order by shipping_fee, id) as shipping_fee_bucket,
        lag(amount, 1) over (partition by status order by delivered_date, id) as prev_amount
    from with_dims
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipping_fee,
        order_status,
        order_date,
        amount,
        order_id_max_amount,
        amount_avg_2,
        first_known_amount,
        shipping_fee_bucket,
        prev_amount,
        avg(cost) over w_running as cost_avg_6,
        first_value(carrier, true) over (order by order_id_max_amount, id rows between unbounded preceding and current row) as first_known_carrier
    from metrics
    window w_running as (partition by status order by shipping_fee_bucket, id rows between 6 preceding and current row)
)
select
    id,
    carrier,
    delivered_date,
    cost,
    shipping_fee,
    order_status,
    order_date,
    amount,
    order_id_max_amount
from with_windows
order by order_status desc nulls last;
