with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at asc, id asc) > interval 30 days then 1 else 0 end as is_new_session
    from sessions
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by customer_id order by started_at asc, id asc rows between unbounded preceding and current row) as session_no
    from gaps
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from sessions_sessions s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time < s.started_at
        )
),
kept_keys as (
    -- Set difference on keys
    select country, browser
    from eligible
    except distinct
    select country, browser
    from sessions
    where to_date(started_at) < '2024-12-09'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from eligible s
    inner join kept_keys k on s.country = k.country and s.browser = k.browser
),
joined as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.device,
        src.browser,
        src.started_at,
        src.is_new_session,
        d.city,
        d.age
    from reconciled src
    inner join customers d
        on src.customer_id = d.id
)
-- Grouped metrics
select
    case when is_new_session >= 1 then 'LARGE' else 'SMALL' end as is_new_session_tier,
    date_trunc('week', started_at) as week_start,
    date_trunc('month', started_at) as activity_month,
    count(*) as num_rows,
    round(avg(age), 2) as avg_age,
    count(case when device = 'mobile' then 1 end) as mobile_count,
    min(started_at) as started_at,
    count_if(city = 'London') as london_events,
    count(distinct browser) as distinct_browsers,
    grouping(case when is_new_session >= 1 then 'LARGE' else 'SMALL' end) as grouping_id
from joined
group by rollup (case when is_new_session >= 1 then 'LARGE' else 'SMALL' end, date_trunc('week', started_at), date_trunc('month', started_at))
having mobile_count >= 1
