with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by browser order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by browser order by started_at, id) + interval '60' day then 1 else 0 end as new_group_flag
    from sessions
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as session_id
    from flagged
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(10) over (partition by customer_id order by session_id desc, id) as session_id_bucket,
        sum(new_group_flag) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as running_new_group_flag,
        sum(new_group_flag) over (partition by browser order by session_id, id rows between unbounded preceding and current row) as cumulative_new_group_flag,
        avg(new_group_flag) over (partition by country order by session_id, id rows between 6 preceding and 1 preceding) as new_group_flag_avg_6_prior,
        lag(new_group_flag, 1, 0) over (partition by browser order by session_id, id) as prev_new_group_flag
    from sessions_sessions
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from enriched
    qualify row_number() over (partition by device, country order by running_new_group_flag desc, id) = 1
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        new_group_flag,
        session_id,
        session_id_bucket,
        running_new_group_flag,
        cumulative_new_group_flag,
        new_group_flag_avg_6_prior,
        prev_new_group_flag,
        lead(new_group_flag, 2) over (partition by browser, device order by cumulative_new_group_flag, id) as next_new_group_flag,
        ntile(4) over (partition by browser order by prev_new_group_flag desc, id) as prev_new_group_flag_bucket,
        avg(prev_new_group_flag) over (partition by browser) as browser_avg_prev_new_group_flag
    from ranked
)
select
    id,
    device,
    browser,
    session_id,
    running_new_group_flag,
    cumulative_new_group_flag,
    new_group_flag_avg_6_prior,
    prev_new_group_flag,
    next_new_group_flag
from with_windows
order by device desc nulls last, browser desc
