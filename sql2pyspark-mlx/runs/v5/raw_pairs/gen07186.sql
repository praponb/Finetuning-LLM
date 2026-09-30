with valid as (
    select
        id,
        customer_id,
        device,
        started_at,
        country,
        day(started_at) as started_at_day
    from sessions
    where country is not null
      and started_at >= '2023-10-01'
),
kept_keys as (
    select id, country
    from valid
    intersect all
    select id, country
    from sessions
    where country in ('US', 'UK')
),
verified as (
    select s.*
    from valid s
    join kept_keys k on s.id = k.id and s.country = k.country
),
kept as (
    select s.*
    from verified s
    where exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and hour(x.event_time) between 10 and 17
        )
),
kept_keys_2 as (
    select id, country
    from kept
    except all
    select id, country
    from sessions
    where device = 'mobile'
),
kept_rows as (
    select s.*
    from kept s
    join kept_keys_2 k on s.id = k.id and s.country = k.country
),
not_flagged as (
    select s.*
    from kept_rows s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time >= s.started_at
        )
),
with_dims as (
    select e.*, b.email
    from not_flagged e
    left join customers b
        on e.customer_id = b.id
),
metrics as (
    select *, max(started_at_day) over (partition by device) as device_max_started_at_day
    from with_dims
    where customer_id in (16, 28, 30)
),
stats as (
    select
        date_trunc('week', started_at) as week_start,
        customer_id,
        percentile_approx(started_at_day, 0.9) as started_at_day_p90,
        count(*) filter (where country = 'JP') as jp_rows
    from metrics
    group by grouping sets ((date_trunc('week', started_at), customer_id), (date_trunc('week', started_at)), ())
)
select
    s.customer_id,
    s.started_at_day_p90,
    (select min(x.monthly_fee) from subscriptions x where x.customer_id = s.customer_id) as min_subscription_monthly_fee
from stats s
