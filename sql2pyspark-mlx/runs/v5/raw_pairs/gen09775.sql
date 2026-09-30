with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        page,
        event_time,
        date_trunc('WEEK', event_time) event_time_period,
        cast(duration_sec as double) duration_sec_num
    from web_events
    where page is not null
      and event_time >= date '2023-10-21'
),
joined as (
    -- Attach sessions attributes
    select
        e.*,
        d.device,
        d.browser,
        d.country
    from valid e
    left join sessions d
        on e.session_id = d.id
    where d.device = 'mobile'
),
kept_keys as (
    -- Set difference on keys
    select session_id
    from joined
    except
    select session_id
    from web_events
    where session_id in (6, 10, 12, 14)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join kept_keys k on s.session_id = k.session_id
),
with_windows as (
    -- Running and moving aggregates
    select *, sum(duration_sec_num) over (partition by session_id) session_id_sum_duration_sec_num
    from verified
)
-- Join a dimension and aggregate
select
    dim.browser as session_browser,
    dim.country as session_country,
    a.country as web_event_country,
    count(*) as cnt,
    round(avg(a.session_id_sum_duration_sec_num), 2) as avg_session_id_sum_duration_sec_num,
    max(a.duration_sec_num) as max_duration_sec_num
from with_windows a
left join sessions dim on a.session_id = dim.id
group by rollup (dim.browser, dim.country, a.country)
order by web_event_country desc nulls first;
