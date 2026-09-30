with scored as (
    select
        id,
        customer_id,
        plan,
        monthly_fee,
        status,
        count(monthly_fee) over w_grp as running_count_monthly_fee,
        min(monthly_fee) over w_grp as running_min_monthly_fee,
        avg(monthly_fee) over w_grp as running_avg_monthly_fee
    from subscriptions
    where status not in ('ACTIVE', 'PAUSED')
    window w_grp as (partition by plan order by start_date asc, id asc rows between unbounded preceding and current row)
),
latest as (
    select *, row_number() over (partition by status order by running_count_monthly_fee desc, id asc) as row_num
    from scored
    qualify row_num = 1
),
ranked as (
    select
        id,
        customer_id,
        monthly_fee,
        status,
        running_count_monthly_fee
    from (
        select *, row_number() over (partition by plan order by running_count_monthly_fee desc, id asc) as rn
        from latest
    ) t
    where rn = 1
),
base as (
    select
        *,
        monthly_fee * running_count_monthly_fee as monthly_fee_times_running_count_monthly_fee,
        monthly_fee - running_count_monthly_fee as monthly_fee_minus_running_count_monthly_fee
    from ranked
    where status != 'CANCELLED'
),
latest_subscriptions as (
    select
        id,
        status,
        running_count_monthly_fee,
        monthly_fee_times_running_count_monthly_fee,
        monthly_fee_minus_running_count_monthly_fee
    from (
        select *, row_number() over (partition by status order by monthly_fee asc nulls last, id asc) as rn
        from base
    ) x
    where rn = 1
),
latest_subscriptions_2 as (
    select *
    from (
        select *, rank() over (partition by status order by monthly_fee_minus_running_count_monthly_fee desc nulls first) as dedup_rank
        from latest_subscriptions
    ) x
    where dedup_rank = 1
),
daily as (
    select
        case when running_count_monthly_fee >= 14 then 'HIGH' else 'LOW' end as running_count_monthly_fee_tier,
        sum(case when status = 'PAUSED' then monthly_fee_minus_running_count_monthly_fee else 0 end) as paused_monthly_fee_minus_running_count_monthly_fee,
        avg(running_count_monthly_fee) as avg_running_count_monthly_fee,
        count_if(status = 'CANCELLED') as cancelled_events,
        approx_percentile(monthly_fee_minus_running_count_monthly_fee, 0.9) as monthly_fee_minus_running_count_monthly_fee_p90,
        approx_percentile(monthly_fee_times_running_count_monthly_fee, array(0.25, 0.5, 0.75)) as monthly_fee_times_running_count_monthly_fee_quartiles,
        avg(monthly_fee_times_running_count_monthly_fee) as avg_monthly_fee_times_running_count_monthly_fee
    from latest_subscriptions_2
    group by cube (case when running_count_monthly_fee >= 14 then 'HIGH' else 'LOW' end)
)
select
    running_count_monthly_fee_tier,
    paused_monthly_fee_minus_running_count_monthly_fee,
    avg_running_count_monthly_fee,
    cancelled_events,
    monthly_fee_minus_running_count_monthly_fee_p90,
    monthly_fee_times_running_count_monthly_fee_quartiles,
    avg_monthly_fee_times_running_count_monthly_fee
from daily
order by monthly_fee_minus_running_count_monthly_fee_p90 desc nulls last
