with latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from sessions
    qualify row_number() over (partition by browser, device order by started_at desc nulls first, id) = 1
),
with_dims as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.browser,
        s.started_at,
        c.signup_date,
        c.country as customer_country
    from latest s
    left join customers c
        on s.customer_id = c.id
    where year(c.signup_date) >= 2024
),
latest_sessions as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        customer_id,
        started_at
    from (
        select *, rank() over (partition by browser order by signup_date desc nulls first) as latest_rank
        from with_dims
    ) x
    where latest_rank = 1
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_sessions s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and to_date(x.event_time) < '2023-07-13'
        )
),
base as (
    -- Keep recent, valid rows
    select *, datediff(current_date(), started_at) as days_since_started_at
    from not_flagged
    where started_at > to_date('2023-05-28')
      and customer_id is not null
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by customer_id order by started_at desc nulls first, id) = 1
),
grouped as (
    -- Grouped metrics
    select
        date_trunc('year', started_at) as year_start,
        approx_percentile(days_since_started_at, array(0.25, 0.5, 0.75)) as days_since_started_at_quartiles,
        (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as duration_min,
        (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as span_minutes
    from ranked
    group by date_trunc('year', started_at)
)
select
    year_start,
    days_since_started_at_quartiles,
    duration_min,
    span_minutes
from grouped;
