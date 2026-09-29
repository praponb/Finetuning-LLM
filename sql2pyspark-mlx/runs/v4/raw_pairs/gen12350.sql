with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from sessions s
    where exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time <= s.started_at
        )
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by device order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by device order by started_at, id) + interval '7' day then 1 else 0 end as session_start_flag
    from kept
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_no
    from flagged
),
with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, x.event_type
    from sessionized s
    join lateral (
            select c.event_type
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by browser order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by browser order by started_at, id) + interval 14 days then 1 else 0 end as is_new_session
    from with_last
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
kept_keys as (
    -- Set difference on keys
    select customer_id
    from islands
    except all
    select customer_id
    from sessions
    where browser in ('Firefox', 'Safari')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from islands s
    inner join kept_keys k on s.customer_id = k.customer_id
),
remaining_keys as (
    -- Set difference on keys
    select browser
    from verified
    except all
    select browser
    from sessions
    where hour(started_at) between 9 and 12
),
verified_sessions as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join remaining_keys k on s.browser = k.browser
),
with_dims as (
    -- Attach customers attributes
    select
        e.*,
        d.segment,
        d.signup_date,
        d.city
    from verified_sessions e
    left join customers d
        on e.customer_id = d.id
),
summary as (
    -- Grouped metrics
    select
        case when session_start_flag >= 1 then 'LARGE' else 'SMALL' end as session_start_flag_tier,
        date_trunc('year', started_at) as year_start,
        event_type,
        count(*) as event_count,
        min(started_at) as first_started_at
    from with_dims
    group by case when session_start_flag >= 1 then 'LARGE' else 'SMALL' end, date_trunc('year', started_at), event_type
    having count(*) > 1
)
select
    session_start_flag_tier,
    year_start,
    event_type,
    event_count,
    first_started_at
from summary
