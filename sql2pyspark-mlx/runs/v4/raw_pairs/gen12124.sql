with scored as (
    -- Window metrics over several different partitions
    select
        *,
        sum(monthly_fee) over (partition by plan, status order by end_date, id rows between unbounded preceding and current row) as running_monthly_fee,
        avg(monthly_fee) over (partition by status order by monthly_fee, id rows between 5 preceding and 1 preceding) as monthly_fee_avg_5_prior,
        cume_dist() over (partition by status order by monthly_fee nulls first) as monthly_fee_cume_dist,
        cume_dist() over (partition by status order by monthly_fee desc) as monthly_fee_cume_dist_2
    from subscriptions
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        running_monthly_fee,
        monthly_fee_avg_5_prior,
        monthly_fee_cume_dist,
        monthly_fee_cume_dist_2,
        first_value(running_monthly_fee, true) over (partition by customer_id order by monthly_fee_cume_dist_2, id rows between unbounded preceding and current row) as first_known_running_monthly_fee,
        ntile(10) over (partition by customer_id order by monthly_fee_cume_dist_2 desc, id) as monthly_fee_cume_dist_2_bucket
    from scored
),
with_windows_subscriptions as (
    -- Running and moving aggregates
    select
        *,
        row_number() over w_grp as event_seq,
        percent_rank() over (partition by customer_id, status order by first_known_running_monthly_fee nulls first) as first_known_running_monthly_fee_percent_rank
    from with_windows
    window w_grp as (partition by plan, customer_id order by monthly_fee_cume_dist_2_bucket, id)
),
base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        end_date,
        monthly_fee,
        monthly_fee_avg_5_prior,
        monthly_fee_cume_dist,
        monthly_fee_cume_dist_2,
        first_known_running_monthly_fee,
        first_known_running_monthly_fee_percent_rank,
        coalesce(monthly_fee_cume_dist_2_bucket, 0) as monthly_fee_cume_dist_2_bucket_filled
    from with_windows_subscriptions
    where end_date > date '2024-01-29'
      and customer_id is not null
      and monthly_fee_cume_dist_2_bucket > 7
)
select
    s.id,
    s.customer_id,
    s.end_date,
    s.monthly_fee_avg_5_prior,
    s.monthly_fee_cume_dist,
    s.first_known_running_monthly_fee,
    s.first_known_running_monthly_fee_percent_rank,
    s.monthly_fee_cume_dist_2_bucket_filled,
    (select sum(o2.amount) from orders o2 where o2.customer_id = s.customer_id) as sum_order_amount
from base s
order by monthly_fee_cume_dist desc nulls first, monthly_fee_cume_dist_2_bucket_filled;
