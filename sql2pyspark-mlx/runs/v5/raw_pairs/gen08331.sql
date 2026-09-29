with cleaned as (
    select
        id,
        device,
        browser,
        started_at,
        country,
        month(started_at) as started_at_month,
        coalesce(country, 'DIRECT') as country_clean
    from sessions
    where started_at > date '2023-08-05'
)
select s.*
from cleaned s
where exists (
        select 1
        from web_events ch
        where ch.session_id = s.id
          and ch.event_time >= s.started_at
    )
