with filtered as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), shipped_date) as days_since_shipped_date,
        coalesce(carrier, 'N/A') as carrier_clean
    from shipments
    where shipped_date > date '2023-06-21'
      and order_id is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        lead(days_since_shipped_date) over w_hist as next_days_since_shipped_date,
        lag(days_since_shipped_date) over w_hist as prev_days_since_shipped_date,
        lead(cost) over w_hist as next_cost,
        row_number() over w_hist as row_seq
    from filtered
    where carrier <> 'Flash'
    window w_hist as (partition by order_id, carrier order by cost, id)
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        delivered_date,
        cost,
        status,
        days_since_shipped_date,
        prev_days_since_shipped_date,
        next_cost,
        row_seq,
        coalesce(prev_days_since_shipped_date, 0) as prev_days_since_shipped_date_filled
    from with_windows
    where carrier is distinct from 'Kerry'
),
joined as (
    -- Attach orders attributes
    select a.*, o.shipping_fee
    from valid a
    left join orders o
        on a.order_id = o.id
),
enriched as (
    -- Attach orders attributes
    select src.*, d.discount
    from joined src
    left join orders d
        on src.order_id = d.id
)
select
    s.id,
    s.order_id,
    s.delivered_date,
    s.cost,
    s.status,
    s.days_since_shipped_date,
    s.prev_days_since_shipped_date,
    s.row_seq,
    s.shipping_fee,
    (select sum(t2.cost) from shipments t2 where t2.order_id = s.order_id) as sum_shipment_cost
from enriched s
