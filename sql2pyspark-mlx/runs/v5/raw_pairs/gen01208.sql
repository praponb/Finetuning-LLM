with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        lower(status) as status_lower,
        upper(carrier) as carrier_upper
    from shipments
    where month(shipped_date) in (6, 10, 12)
),
with_dims as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.carrier,
        src.delivered_date,
        src.cost,
        src.status,
        src.status_lower,
        src.carrier_upper,
        o.amount,
        o.status as order_status
    from filtered src
    join orders o
        on src.order_id = o.id
),
enriched as (
    -- Ranking and gap features
    select
        *,
        avg(cost) over w_ordered as cost_avg_6,
        ntile(4) over (partition by carrier order by amount desc, id) as amount_bucket
    from with_dims
    window w_ordered as (order by amount, id rows between 6 preceding and current row)
),
enriched_shipments as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.delivered_date,
        src.cost,
        src.status,
        src.amount,
        src.order_status,
        src.cost_avg_6,
        b.discount,
        b.shipping_fee
    from enriched src
    left join orders b
        on src.order_id <=> b.id
    where b.discount <= 0.16
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        delivered_date,
        cost,
        status,
        order_status,
        cost_avg_6,
        discount,
        shipping_fee,
        round(cost_avg_6, 2) as cost_avg_6_rounded,
        lower(status) as status_lower
    from enriched_shipments
    where cost_avg_6 between 41 and 46
),
enriched_shipments_2 as (
    -- Running and moving aggregates
    select
        id,
        cost_avg_6,
        discount,
        shipping_fee,
        status_lower,
        row_number() over (partition by order_status, status order by cost_avg_6_rounded, id) as seq_no,
        first_value(cost) ignore nulls over (partition by order_status order by cost_avg_6, id rows between unbounded preceding and current row) as first_known_cost
    from valid
)
select
    id,
    cost_avg_6,
    first_known_cost
from enriched_shipments_2
order by cost_avg_6 nulls last
