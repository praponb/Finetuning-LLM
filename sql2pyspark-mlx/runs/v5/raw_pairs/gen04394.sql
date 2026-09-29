with with_dims as (
    select
        a.id,
        a.customer_id,
        a.plan,
        a.start_date,
        a.monthly_fee,
        a.status,
        p.city,
        p.name,
        p.country
    from subscriptions a
    left join customers p
        on a.customer_id <=> p.id
    where p.city is null
),
metrics as (
    select
        id,
        plan,
        monthly_fee,
        city,
        name,
        country,
        count(monthly_fee) over (partition by country order by monthly_fee, id rows between 3 preceding and current row) last4_count_monthly_fee,
        avg(monthly_fee) over (partition by country order by monthly_fee, id rows between 3 preceding and current row) last4_avg_monthly_fee,
        sum(monthly_fee) over (partition by country order by monthly_fee, id rows between 3 preceding and current row) last4_sum_monthly_fee,
        max(monthly_fee) over (partition by country order by monthly_fee, id rows between 3 preceding and current row) last4_max_monthly_fee
    from with_dims
),
with_windows as (
    select
        id,
        plan,
        monthly_fee,
        city,
        country,
        last4_avg_monthly_fee,
        last4_max_monthly_fee,
        last_value(last4_avg_monthly_fee, true) over (partition by city order by last4_sum_monthly_fee, id rows between unbounded preceding and current row) last_known_last4_avg_monthly_fee,
        ntile(4) over (partition by city order by last4_count_monthly_fee desc, id) last4_count_monthly_fee_bucket,
        sum(last4_sum_monthly_fee) over (partition by country order by last4_avg_monthly_fee, id rows between unbounded preceding and current row) cumulative_last4_sum_monthly_fee
    from metrics
),
metrics_subscriptions as (
    select
        id,
        plan,
        last4_max_monthly_fee,
        last_known_last4_avg_monthly_fee,
        avg(cumulative_last4_sum_monthly_fee) over (partition by country, plan order by last4_max_monthly_fee, id rows between unbounded preceding and current row) running_avg_cumulative_last4_sum_monthly_fee,
        max(cumulative_last4_sum_monthly_fee) over (partition by country, plan order by last4_max_monthly_fee, id rows between unbounded preceding and current row) running_max_cumulative_last4_sum_monthly_fee,
        min(cumulative_last4_sum_monthly_fee) over (partition by country, plan order by last4_max_monthly_fee, id rows between unbounded preceding and current row) running_min_cumulative_last4_sum_monthly_fee
    from with_windows
),
ranked as (
    select *, row_number() over (partition by plan order by running_max_cumulative_last4_sum_monthly_fee desc, id) as row_num
    from metrics_subscriptions
    qualify row_num = 1
)
select
    id,
    plan,
    last4_max_monthly_fee,
    last_known_last4_avg_monthly_fee,
    running_avg_cumulative_last4_sum_monthly_fee,
    running_max_cumulative_last4_sum_monthly_fee,
    row_num
from ranked
