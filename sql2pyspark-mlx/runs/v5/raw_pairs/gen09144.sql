with latest_match as (
    select
        s.*,
        latest.web_event_event_type,
        latest.duration_sec
    from sessions s
    left join lateral (
            select c.event_type as web_event_event_type, c.duration_sec
            from web_events c
            where c.session_id = s.id
            order by c.event_time desc, c.id desc
            limit 1
        ) latest on true
)
select
    date_trunc('quarter', started_at) as quarter_start,
    min(started_at) as first_started_at,
    sort_array(collect_set(device)) as device_list,
    max(started_at) as last_seen,
    sum(duration_sec) filter (where country = 'SG') as sg_total_duration_sec
from latest_match
group by date_trunc('quarter', started_at)
order by quarter_start desc nulls last;
