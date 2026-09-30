with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        carrier,
        cost,
        datediff(delivered_date, shipped_date) as days_shipped_date_to_delivered_date,
        round(cost * 1.15, 2) as adjusted_cost,
        coalesce(carrier, 'OTHER') as carrier_filled
    from shipments
    where delivered_date > date '2023-08-24'
),
joined as (
    -- Attach orders attributes
    select
        a.*,
        b.order_date,
        b.status,
        b.shipping_fee
    from base a
    left join orders b
        on a.order_id = b.id
),
joined_shipments as (
    -- Attach orders attributes
    select e.*, b.status as order_status
    from joined e
    join orders b
        on e.order_id = b.id
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        round(cost / sum(cost) over (partition by status), 4) as cost_share,
        sum(shipping_fee) over (partition by status order by order_date, id rows between unbounded preceding and current row) as cumulative_shipping_fee
    from joined_shipments
)
-- Multi-level aggregation
select
    date_trunc('quarter', order_date) as quarter_start,
    count(*) filter (where status = 'CANCELLED') as cancelled_rows,
    count(*) filter (where order_status = 'CANCELLED') as cancelled_rows_2,
    sort_array(collect_set(order_status)) as order_status_list
from enriched
group by quarter_start
having cancelled_rows_2 >= 1
order by quarter_start desc nulls last
