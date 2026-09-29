with valid as (
    -- Filter the raw rows and derive helper columns
    select *, trim(device) clean_device
    from sessions
    where country in ('JP', 'UK')
),
with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by country order by started_at, id) as prev_started_at
    from valid
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 60 days then 1 else 0 end as is_new_burst
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by country order by started_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.web_event_page,
        x.web_event_event_type
    from islands s
    left join lateral (
            select c.page as web_event_page, c.event_type as web_event_event_type
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        d.email,
        d.country as customer_country
    from with_last src
    left join customers d
        on src.customer_id = d.id
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by browser order by started_at, id) > interval 90 days then 1 else 0 end as is_new_burst_2
    from joined
),
islands_sessions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst_2) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as island_id
    from flagged
),
matched_keys as (
    -- Set difference on keys
    select customer_id, country
    from islands_sessions
    except all
    select customer_id, country
    from sessions
    where country in ('JP', 'US', 'UK')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from islands_sessions s
    join matched_keys k on s.customer_id = k.customer_id and s.country = k.country
),
matched_keys_2 as (
    -- Set intersection on keys
    select browser, device
    from reconciled
    intersect all
    select browser, device
    from sessions
    where started_at >= '2023-05-01'
),
reconciled_sessions as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled s
    join matched_keys_2 k on s.browser = k.browser and s.device = k.device
)
select
    s.id,
    s.customer_id,
    s.started_at,
    s.country,
    s.clean_device,
    s.prev_started_at,
    s.web_event_page,
    s.web_event_event_type,
    s.email,
    case when country rlike '^(US|JP)$' then 'GROUP_B' when country rlike '^(TH|SG)$' then 'CORE' else 'PRIORITY' end as country_zone,
    (select sum(t2.monthly_fee) from subscriptions t2 where t2.customer_id = s.customer_id) as sum_subscription_monthly_fee
from reconciled_sessions s;
