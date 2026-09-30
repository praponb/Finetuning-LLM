with with_latest as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, latest.event_time
    from sessions s
    left join lateral (
            select c.event_time
            from web_events c
            where c.session_id = s.id
            order by c.event_time desc, c.id desc
            limit 1
        ) latest on true
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        p.name,
        p.country as customer_country
    from with_latest src
    join customers p
        on src.customer_id = p.id
),
with_latest_sessions as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.duration_sec,
        latest.web_event_page
    from joined s
    left join lateral (
            select c.duration_sec, c.page as web_event_page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
),
with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, latest.web_event_duration_sec
    from with_latest_sessions s
    join lateral (
            select c.duration_sec as web_event_duration_sec
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
)
-- Grouped metrics
select
    customer_id,
    min(started_at) as started_at,
    approx_percentile(duration_sec, array(0.25, 0.5, 0.75)) as duration_sec_quartiles,
    round(avg(web_event_duration_sec), 2) as avg_web_event_duration_sec,
    count_if(device = 'tablet') as tablet_events,
    round(avg(web_event_duration_sec), 2) as avg_web_event_duration_sec_2,
    max(web_event_duration_sec) as max_web_event_duration_sec
from with_last
group by customer_id
having tablet_events < 1000
order by avg_web_event_duration_sec_2;
