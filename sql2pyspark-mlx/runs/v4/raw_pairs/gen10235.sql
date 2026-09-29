with base as (
    select
        *,
        to_date(started_at) as started_at_date,
        coalesce(country, 'NONE') as country_clean
    from sessions
    where started_at > timestamp '2023-04-24 02:00:00'
      and customer_id is not null
      and device in ('desktop', 'mobile', 'tablet')
),
matched_keys as (
    select id
    from base
    except all
    select id
    from sessions
    where customer_id is null
),
kept_rows as (
    select s.*
    from base s
    inner join matched_keys k on s.id = k.id
),
kept as (
    select s.*
    from kept_rows s
    where exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_type = 'search'
        )
),
joined as (
    select
        a.*,
        p.city,
        p.age
    from kept a
    inner join customers p
        on a.customer_id = p.id
),
remaining_keys as (
    select browser, country
    from joined
    except all
    select browser, country
    from sessions
    where device in ('desktop', 'mobile', 'tablet')
),
kept_rows_sessions as (
    select s.*
    from joined s
    inner join remaining_keys k on s.browser = k.browser and s.country = k.country
)
select
    device,
    browser,
    started_at,
    country,
    started_at_date,
    country_clean,
    age,
    case when browser rlike '^(Chrome|Firefox)$' then 'CORE' else 'PRIORITY' end as browser_zone
from kept_rows_sessions
order by started_at desc
