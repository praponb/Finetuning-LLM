with valid as (
    -- Basic cleansing and defaults
    select
        *,
        add_months(shipped_date, 6) as shipped_date_plus_months,
        coalesce(carrier, 'OTHER') as resolved_carrier
    from shipments
    where order_id is not null
      and cost is null
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lag(cost, 2, 0) over (order by cost, id) as prev_cost,
        round(cost / sum(cost) over (partition by order_id), 4) as cost_share,
        max(cost) over (partition by carrier) as carrier_max_cost
    from valid
    where order_id is not null
),
enriched as (
    -- Attach orders attributes
    select
        s.*,
        d.shipping_fee,
        d.amount
    from metrics s
    inner join orders d
        on s.order_id <=> d.id
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        delivered_date,
        cost,
        status,
        shipped_date_plus_months,
        resolved_carrier,
        prev_cost,
        cost_share,
        carrier_max_cost,
        shipping_fee
    from (
        select *, row_number() over (partition by order_id order by cost_share, id) as row_num
        from enriched
    ) x
    where row_num = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        status,
        shipped_date_plus_months,
        cost_share,
        row_number() over (partition by status order by delivered_date desc, id) as row_seq,
        sum(cost) over (partition by status order by cost, id rows between unbounded preceding and current row) as running_cost
    from deduped
),
metrics_shipments as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(cost_share, true) over (partition by status order by running_cost, id rows between unbounded preceding and current row) as first_known_cost_share,
        rank() over (partition by status order by running_cost desc nulls first) as running_cost_rank
    from with_windows
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from metrics_shipments
    qualify row_number() over (partition by status order by running_cost_rank desc nulls first, id) = 1
)
-- Multi-level aggregation
select
    status,
    date_trunc('year', shipped_date_plus_months) as year_start,
    case when first_known_cost_share >= 0.83 then 'PREMIUM' else 'STANDARD' end as first_known_cost_share_tier,
    round(sum(first_known_cost_share) / nullif(count(case when status = 'DELIVERED' then 1 end), 0), 2) as first_known_cost_share_per_delivered,
    count(*) as row_count,
    round(avg(cost_share), 2) as avg_cost_share,
    grouping_id() as grouping_id
from latest
group by cube (status, date_trunc('year', shipped_date_plus_months), case when first_known_cost_share >= 0.83 then 'PREMIUM' else 'STANDARD' end)
order by first_known_cost_share_per_delivered nulls first
