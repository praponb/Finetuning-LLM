with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        device,
        browser,
        country,
        upper(device) as device_upper
    from sessions
    where started_at > date '2024-01-18'
      and customer_id in (15, 20, 32, 37)
)
-- Latest matching web_event per row (LATERAL ... LIMIT 1)
select
    s.*,
    x.web_event_duration_sec,
    x.web_event_page
from cleaned s
left join lateral (
        select c.duration_sec as web_event_duration_sec, c.page as web_event_page
        from web_events c
        where c.session_id = s.id
        order by c.duration_sec desc, c.id desc
        limit 1
    ) x on true;
