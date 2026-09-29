with matched_keys as (
    -- Set difference on keys
    select id, browser
    from sessions
    except
    select id, browser
    from sessions
    where country not in ('JP', 'SG')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions s
    inner join matched_keys k on s.id = k.id and s.browser = k.browser
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept_rows s
    where exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time < s.started_at
        )
),
kept_sessions as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time < '2023-07-01'
        )
),
joined as (
    -- Attach customers attributes
    select
        a.customer_id,
        a.browser,
        a.country,
        d.segment
    from kept_sessions a
    left join customers d
        on a.customer_id = d.id
),
with_dims as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.browser,
        b.age,
        b.name,
        b.country as customer_country
    from joined e
    left join customers b
        on e.customer_id = b.id
    where month(b.signup_date) in (2, 4, 8)
)
select
    s.customer_id,
    s.browser,
    s.age,
    s.name,
    s.customer_country,
    (select avg(x.discount) from orders x where x.customer_id = s.customer_id) as avg_order_discount
from with_dims s
order by browser, avg_order_discount
