with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from customers s
    where exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.opened_date < s.signup_date
        )
),
with_latest as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.country,
        s.city,
        s.segment,
        s.credit_limit,
        s.age,
        x.end_date,
        x.subscription_start_date
    from kept s
    join lateral (
            select c.end_date, c.start_date as subscription_start_date
            from subscriptions c
            where c.customer_id = s.id
              and c.start_date <= s.signup_date
            order by c.start_date desc, c.id desc
            limit 1
        ) x on true
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select id, segment
    from with_latest
    qualify row_number() over (partition by city order by credit_limit nulls first, id) = 1
),
valid as (
    -- Keep recent, valid rows
    select *, case when segment in ('Consumer', 'Corporate') then 'Group A' else 'Other' end as segment_group
    from ranked
    where segment is not null
      and segment is distinct from 'Consumer'
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.channel = 'partner'
        )
),
eligible_customers as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.customer_id is not null
        )
)
-- Grouped metrics
select
    segment,
    count(*) as event_count,
    count(case when segment = 'Consumer' then 1 end) as consumer_count,
    count(distinct segment) as distinct_segments,
    count_if(segment = 'Consumer') as consumer_events
from eligible_customers
group by 1
order by distinct_segments;
