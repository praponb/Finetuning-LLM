with valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        case when carrier in ('Flash', 'FedEx') then 'Core' else 'Other' end as carrier_group
    from shipments
    where carrier is not null
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by carrier order by shipped_date desc nulls last, id asc) as dedup_rank
        from valid
    ) t
    where dedup_rank = 1
),
joined as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.shipped_date,
        d.amount,
        d.discount,
        d.order_date
    from ranked a
    inner join orders d
        on a.order_id = d.id
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(discount) over w_recent as running_sum_discount,
        count(discount) over w_recent as running_count_discount,
        max(discount) over w_recent as running_max_discount,
        row_number() over w_hist as event_seq
    from joined
    where discount >= 0.12
    window w_recent as (partition by order_id order by shipped_date asc, id asc rows between unbounded preceding and current row), w_hist as (partition by order_id order by discount desc, id asc)
)
-- Aggregate with conditional metrics
select
    order_id,
    case when discount >= 0.44 then 'PREMIUM' else 'STANDARD' end as discount_tier,
    max(running_sum_discount) as max_running_sum_discount,
    percentile_approx(running_sum_discount, 0.9) as running_sum_discount_p90,
    count(distinct id) as distinct_ids,
    count(distinct order_id) as distinct_order_ids
from with_windows
group by order_id, discount_tier
