with filtered as (
    select *, upper(status) as status_upper
    from shipments
    where shipped_date > date '2023-11-01'
),
kept_keys as (
    select carrier
    from filtered
    intersect
    select carrier
    from shipments
    where cost >= 79
),
reconciled as (
    select s.*
    from filtered s
    join kept_keys k on s.carrier = k.carrier
),
with_windows as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_upper,
        cume_dist() over (partition by status order by cost desc) as cost_cume_dist,
        percent_rank() over (partition by status order by cost desc nulls last) as cost_percent_rank,
        rank() over (partition by carrier order by cost nulls last) as cost_rank,
        dense_rank() over w_part as cost_dense_rank,
        percent_rank() over (partition by status order by cost nulls first) as cost_percent_rank_2
    from reconciled
    window w_part as (order by cost desc nulls last)
),
filtered_shipments as (
    select
        carrier,
        shipped_date,
        delivered_date,
        status,
        cost_percent_rank,
        cost_rank,
        cost_dense_rank,
        cost_percent_rank_2,
        case when cost_cume_dist >= 0.65 then 'High' when cost_cume_dist >= 0.13 then 'Medium' else 'Low' end as cost_cume_dist_level,
        coalesce(carrier, 'NONE') as resolved_carrier
    from with_windows
    where shipped_date >= date '2023-12-02'
      and cost < 53
)
select
    delivered_date,
    status,
    cost_percent_rank,
    cost_rank,
    cost_dense_rank,
    cost_percent_rank_2,
    cost_cume_dist_level,
    resolved_carrier
from filtered_shipments;
