with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        floor(monthly_fee) as monthly_fee_whole
    from subscriptions
    where start_date >= to_date('2023-05-10')
      and customer_id is not null
),
kept_keys as (
    -- Set difference on keys
    select plan, customer_id
    from filtered
    except
    select plan, customer_id
    from subscriptions
    where status in ('EXPIRED', 'PAUSED')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join kept_keys k on s.plan = k.plan and s.customer_id = k.customer_id
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(monthly_fee_whole, 0) as monthly_fee_whole_filled,
        add_months(end_date, 6) as end_date_plus_months
    from kept_rows
    where end_date > date '2023-09-29'
      and customer_id is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        plan,
        end_date,
        monthly_fee_whole_filled,
        end_date_plus_months,
        cume_dist() over w_grp as monthly_fee_whole_cume_dist,
        percent_rank() over (partition by customer_id order by monthly_fee_whole desc) as monthly_fee_whole_percent_rank
    from cleaned
    where monthly_fee <= 90
    window w_grp as (partition by plan order by monthly_fee_whole nulls first)
),
with_windows_subscriptions as (
    -- Ranking and gap features
    select
        *,
        max(monthly_fee_whole_percent_rank) over (partition by plan) as plan_max_monthly_fee_whole_percent_rank,
        dense_rank() over (partition by plan order by monthly_fee_whole_filled desc) as monthly_fee_whole_filled_dense_rank,
        avg(monthly_fee_whole_filled) over (partition by plan) as plan_avg_monthly_fee_whole_filled,
        cume_dist() over (partition by plan order by monthly_fee_whole_percent_rank) as monthly_fee_whole_percent_rank_cume_dist
    from with_windows
),
with_windows_subscriptions_2 as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by plan order by monthly_fee_whole_cume_dist desc nulls first) as monthly_fee_whole_cume_dist_percent_rank,
        sum(monthly_fee_whole_percent_rank_cume_dist) over (partition by plan) as plan_sum_monthly_fee_whole_percent_rank_cume_dist,
        percent_rank() over (order by plan_max_monthly_fee_whole_percent_rank desc) as plan_max_monthly_fee_whole_percent_rank_percent_rank
    from with_windows_subscriptions
),
cleaned_subscriptions as (
    -- Keep recent, valid rows
    select *, case when plan_max_monthly_fee_whole_percent_rank_percent_rank >= 0.66 then 'High' when plan_max_monthly_fee_whole_percent_rank_percent_rank >= 0.21 then 'Medium' else 'Low' end as plan_max_monthly_fee_whole_percent_rank_percent_rank_tier
    from with_windows_subscriptions_2
    where monthly_fee_whole_cume_dist > 0.7
)
-- Multi-level aggregation
select
    date_trunc('quarter', end_date_plus_months) as quarter_start,
    count(distinct id) as distinct_ids,
    count(*) filter (where plan = 'basic') as basic_rows,
    round(avg(monthly_fee_whole_cume_dist), 2) as avg_monthly_fee_whole_cume_dist
from cleaned_subscriptions
group by quarter_start
having basic_rows < 1000
order by distinct_ids desc;
