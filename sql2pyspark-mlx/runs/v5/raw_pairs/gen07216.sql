with filtered as (
    -- Basic cleansing and defaults
    select *, lower(device) device_lower
    from sessions
    where browser is not null
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        p.email,
        p.city,
        p.credit_limit
    from filtered a
    left join customers p
        on a.customer_id = p.id
    where p.is_active = true
),
enriched_sessions as (
    -- Window metrics over several different partitions
    select
        *,
        count(credit_limit) over (partition by browser, device order by started_at, id rows between unbounded preceding and current row) running_count_credit_limit,
        min(credit_limit) over (partition by browser, device order by started_at, id rows between unbounded preceding and current row) running_min_credit_limit,
        max(credit_limit) over (partition by browser, device order by started_at, id rows between unbounded preceding and current row) running_max_credit_limit,
        avg(credit_limit) over (partition by browser, device order by started_at, id rows between unbounded preceding and current row) running_avg_credit_limit,
        stddev_samp(credit_limit) over (partition by device, customer_id) device_stddev_samp_credit_limit
    from enriched
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, latest.web_event_page
    from enriched_sessions s
    left join lateral (
            select c.page as web_event_page
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) latest on true
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
        )
),
latest_match_sessions as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, latest.event_type
    from not_flagged s
    left join lateral (
            select c.event_type
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match_sessions s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time <= s.started_at
        )
)
select
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.email,
    s.credit_limit,
    s.running_min_credit_limit,
    s.running_max_credit_limit,
    (select max(t2.monthly_fee) from subscriptions t2 where t2.customer_id = s.customer_id) as max_subscription_monthly_fee
from kept s;
