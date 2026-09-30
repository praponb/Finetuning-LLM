with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        datediff(shipped_date, delivered_date) as days_delivered_date_to_shipped_date,
        upper(carrier) as carrier_upper,
        coalesce(carrier, 'N/A') as resolved_carrier
    from shipments
    where order_id is not null
      and shipped_date > date '2023-08-06'
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        shipped_date,
        status,
        carrier_upper,
        resolved_carrier,
        cost * days_delivered_date_to_shipped_date as cost_times_days_delivered_date_to_shipped_date,
        weekofyear(shipped_date) as shipped_date_weekofyear
    from cleaned
    where status not in ('IN_TRANSIT', 'LOST')
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(cost_times_days_delivered_date_to_shipped_date) over (partition by order_id, status order by cost_times_days_delivered_date_to_shipped_date, id rows between unbounded preceding and current row) as running_sum_cost_times_days_delivered_date_to_shipped_date,
        count(cost_times_days_delivered_date_to_shipped_date) over (partition by order_id, status order by cost_times_days_delivered_date_to_shipped_date, id rows between unbounded preceding and current row) as running_count_cost_times_days_delivered_date_to_shipped_date,
        avg(cost_times_days_delivered_date_to_shipped_date) over (partition by order_id, status order by cost_times_days_delivered_date_to_shipped_date, id rows between unbounded preceding and current row) as running_avg_cost_times_days_delivered_date_to_shipped_date,
        lead(shipped_date_weekofyear, 1, 0) over (partition by order_id order by cost_times_days_delivered_date_to_shipped_date, id) as next_shipped_date_weekofyear
    from valid
),
joined as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.shipped_date,
        s.status,
        s.carrier_upper,
        s.cost_times_days_delivered_date_to_shipped_date,
        p.shipping_fee
    from enriched s
    inner join orders p
        on s.order_id = p.id
),
enriched_shipments as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by order_id, status order by shipping_fee desc, id) as event_seq,
        ntile(5) over (partition by order_id, status order by cost_times_days_delivered_date_to_shipped_date desc, id) as cost_times_days_delivered_date_to_shipped_date_bucket
    from joined
)
-- Multi-level aggregation
select
    case when cost_times_days_delivered_date_to_shipped_date >= 90 then 'HIGH' else 'LOW' end as cost_times_days_delivered_date_to_shipped_date_tier,
    status,
    round(avg(shipping_fee), 2) as avg_shipping_fee,
    count(distinct id) as distinct_ids,
    approx_percentile(event_seq, 0.95) as event_seq_p95,
    count_if(status = 'IN_TRANSIT') as in_transit_events,
    grouping(case when cost_times_days_delivered_date_to_shipped_date >= 90 then 'HIGH' else 'LOW' end) as gid
from enriched_shipments
group by rollup (case when cost_times_days_delivered_date_to_shipped_date >= 90 then 'HIGH' else 'LOW' end, status)
