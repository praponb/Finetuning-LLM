with base as (
    -- Basic cleansing and defaults
    select *, length(country) as country_length
    from sessions
    where started_at >= timestamp '2023-05-08 18:00:00'
      and started_at >= '2023-12-13'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time < s.started_at
        )
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from not_flagged
    qualify row_number() over (partition by customer_id order by started_at, id) = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by customer_id order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by customer_id order by started_at, id) + interval '7' day then 1 else 0 end as is_new_session
    from latest
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_no
    from flagged
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from islands s
    where exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time >= s.started_at
        )
)
select
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.country_length,
    s.session_no,
    case when browser rlike '^(Chrome|Edge)$' then 'CORE' else 'GROUP_B' end as browser_zone,
    (select sum(s2.monthly_fee) from subscriptions s2 where s2.customer_id = s.customer_id) as sum_subscription_monthly_fee
from eligible s
order by country_length desc, browser_zone;
