with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(is_active as int) as is_active_flag,
        date_trunc('WEEK', signup_date) as signup_date_period
    from customers
    where city is not null
),
prev_events as (
    -- Previous event time per key
    select *, lag(signup_date_period) over (partition by country order by signup_date_period, id) as prev_signup_date_period
    from cleaned
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_signup_date_period is null or signup_date_period > prev_signup_date_period + interval 14 days then 1 else 0 end as session_start_flag
    from prev_events
),
sessions_customers as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by country order by signup_date_period, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
matched_keys as (
    -- Set difference on keys
    select id
    from sessions_customers
    except all
    select id
    from customers
    where upper(trim(email)) = 'DAVID.LEE@GMAIL.COM'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions_customers s
    inner join matched_keys k on s.id = k.id
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(session_start_flag) over (partition by segment) as segment_stddev_samp_session_start_flag,
        dense_rank() over (partition by city, country order by credit_limit nulls first) as credit_limit_dense_rank,
        sum(age) over (partition by country) as country_sum_age,
        rank() over (partition by segment, city order by is_active_flag nulls first) as is_active_flag_rank
    from reconciled
)
-- Window metrics over several different partitions
select
    *,
    avg(session_start_flag) over w_ordered as country_avg_session_start_flag,
    avg(credit_limit) over (partition by country order by cast(prev_signup_date_period as bigint) range between 604800 preceding and current row) as credit_limit_avg_7d,
    avg(is_active_flag) over (partition by country order by cast(prev_signup_date_period as bigint) range between 2592000 preceding and current row) as is_active_flag_avg_30d,
    max(is_active_flag) over (partition by segment) as segment_max_is_active_flag
from enriched
window w_ordered as (partition by country)
