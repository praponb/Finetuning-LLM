with deduped as (
    select
        id,
        customer_id,
        device,
        country
    from sessions
    qualify row_number() over (partition by customer_id order by started_at desc nulls first, id) = 1
),
with_last as (
    select
        s.id,
        s.device,
        s.country,
        latest.duration_sec,
        latest.event_time
    from deduped s
    left join lateral (
            select c.duration_sec, c.event_time
            from web_events c
            where c.session_id = s.id
            order by c.event_time desc, c.id desc
            limit 1
        ) latest on true
),
base as (
    select
        *,
        substring(country, 1, 4) as country_prefix,
        coalesce(country, 'DIRECT') as resolved_country
    from with_last
    where country is not null
),
latest_match as (
    select
        s.id,
        s.device,
        s.duration_sec,
        s.country_prefix,
        s.resolved_country,
        top1.event_type
    from base s
    left join lateral (
            select c.event_type
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) top1 on true
),
with_latest as (
    select
        s.*,
        top1.web_event_event_time,
        top1.page
    from latest_match s
    join lateral (
            select c.event_time as web_event_event_time, c.page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) top1 on true
),
with_last_sessions as (
    select
        s.id,
        s.device,
        s.web_event_event_time,
        s.page,
        latest.web_event_duration_sec,
        latest.web_event_page
    from with_latest s
    left join lateral (
            select c.duration_sec as web_event_duration_sec, c.page as web_event_page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
),
eligible as (
    select s.*
    from with_last_sessions s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time >= s.web_event_event_time
        )
),
stats as (
    select
        web_event_page,
        sum(case when device = 'mobile' then web_event_duration_sec else 0 end) as mobile_web_event_duration_sec,
        approx_percentile(web_event_duration_sec, 0.5) as web_event_duration_sec_p50,
        count(case when web_event_page = '/home' then 1 end) as home_count,
        min(web_event_event_time) as started_at,
        size(collect_set(device)) as num_devices
    from eligible
    group by web_event_page
    having num_devices > 1
)
select
    web_event_page,
    web_event_duration_sec_p50,
    home_count,
    num_devices
from stats
