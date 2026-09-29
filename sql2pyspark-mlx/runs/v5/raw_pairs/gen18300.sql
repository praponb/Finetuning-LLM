with valid as (
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        datediff(current_date(), paid_at) as days_since_paid_at
    from payments
    where order_id is not null
      and order_id is not null
),
with_windows as (
    select
        id,
        method,
        paid_at,
        status,
        days_since_paid_at,
        avg(days_since_paid_at) over (partition by order_id order by days_since_paid_at, id rows between 5 preceding and 1 preceding) as days_since_paid_at_avg_5_prior,
        ntile(10) over (partition by order_id order by days_since_paid_at desc, id) as days_since_paid_at_bucket,
        avg(days_since_paid_at) over (order by days_since_paid_at, id rows between 6 preceding and 1 preceding) as days_since_paid_at_avg_6_prior,
        max(days_since_paid_at) over (partition by status order by cast(paid_at as bigint) range between 2592000 preceding and current row) as days_since_paid_at_max_30d
    from valid
),
scored as (
    select
        id,
        method,
        paid_at,
        status,
        days_since_paid_at_bucket,
        days_since_paid_at_max_30d,
        min(days_since_paid_at) over (partition by status order by days_since_paid_at_avg_6_prior, id rows between 3 preceding and current row) as last4_min_days_since_paid_at,
        max(days_since_paid_at) over (partition by status order by days_since_paid_at_avg_6_prior, id rows between 3 preceding and current row) as last4_max_days_since_paid_at,
        count(days_since_paid_at) over (partition by status order by days_since_paid_at_avg_6_prior, id rows between 3 preceding and current row) as last4_count_days_since_paid_at,
        avg(days_since_paid_at_avg_6_prior) over (order by cast(paid_at as bigint) range between 604800 preceding and current row) as days_since_paid_at_avg_6_prior_avg_7d
    from with_windows
),
with_prev as (
    select *, lag(paid_at) over (partition by status order by paid_at, id) as prev_paid_at
    from scored
),
flagged as (
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '90' day then 1 else 0 end as new_group_flag
    from with_prev
),
islands as (
    select
        id,
        paid_at,
        status,
        days_since_paid_at_max_30d,
        last4_min_days_since_paid_at,
        last4_max_days_since_paid_at,
        days_since_paid_at_avg_6_prior_avg_7d,
        sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as island_id
    from flagged
),
latest as (
    select
        id,
        last4_min_days_since_paid_at,
        last4_max_days_since_paid_at,
        island_id
    from (
        select *, rank() over (partition by status order by days_since_paid_at_max_30d desc nulls last) as rn
        from islands
    ) x
    where rn = 1
)
select
    case when last4_min_days_since_paid_at >= 67 then 'HIGH' else 'LOW' end as last4_min_days_since_paid_at_tier,
    count(*) as row_count,
    avg(last4_min_days_since_paid_at) as avg_last4_min_days_since_paid_at,
    count(*) as row_count_2,
    round(avg(last4_min_days_since_paid_at), 2) as avg_last4_min_days_since_paid_at_2,
    count(distinct id) as distinct_ids
from latest
group by 1
order by distinct_ids desc nulls last;
