with valid as (
    select *, date_trunc('WEEK', start_date) start_date_period
    from subscriptions
    where plan in ('enterprise', 'basic')
),
flagged as (
    select *, case when lag(start_date_period) over (partition by customer_id order by start_date_period, id) is null
                 or start_date_period > lag(start_date_period) over (partition by customer_id order by start_date_period, id) + interval 30 days then 1 else 0 end as session_start_flag
    from valid
),
islands as (
    select *, sum(session_start_flag) over (partition by customer_id order by start_date_period, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
latest as (
    select *, rank() over (partition by plan, status order by end_date desc nulls first) as rank_in_group
    from islands
    qualify rank_in_group = 1
),
gaps as (
    select *, case when lag(start_date_period) over (partition by customer_id order by start_date_period, id) is null
                 or start_date_period > lag(start_date_period) over (partition by customer_id order by start_date_period, id) + interval 30 days then 1 else 0 end as is_new_session
    from latest
),
sessions_subscriptions as (
    select *, sum(is_new_session) over (partition by customer_id order by start_date_period, id rows between unbounded preceding and current row) as burst_id_2
    from gaps
),
with_windows as (
    select
        *,
        lead(is_new_session, 2) over (partition by customer_id order by end_date, id) next_is_new_session,
        ntile(10) over (partition by customer_id order by is_new_session desc, id) is_new_session_bucket,
        sum(monthly_fee) over (partition by customer_id order by cast(start_date_period as bigint) range between 2592000 preceding and current row) monthly_fee_sum_30d,
        sum(monthly_fee) over (partition by status, customer_id order by monthly_fee, id rows between unbounded preceding and current row) running_monthly_fee
    from sessions_subscriptions
    where rank_in_group != 1
),
cleaned as (
    select
        id,
        customer_id,
        plan,
        start_date,
        status,
        start_date_period,
        session_start_flag,
        burst_id,
        rank_in_group,
        is_new_session,
        burst_id_2,
        next_is_new_session,
        running_monthly_fee,
        concat_ws(',', status, plan) status_plan_key
    from with_windows
    where next_is_new_session = 0
)
select
    p.segment,
    s.plan,
    count(*) as cnt,
    min(s.burst_id) as min_burst_id
from cleaned s
left join customers p on s.customer_id = p.id
group by p.segment, s.plan
