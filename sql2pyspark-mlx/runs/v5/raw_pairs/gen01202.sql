with valid as (
    -- Basic cleansing and defaults
    select
        id,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        case when country in ('UK', 'JP') then 'Priority' else 'Other' end as country_group
    from customers
    where segment in ('SMB', 'Corporate')
),
latest_match as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, latest.discount
    from valid s
    left join lateral (
            select c.discount
            from orders c
            where c.customer_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        email,
        segment,
        credit_limit,
        age,
        min(age) over (partition by city order by signup_date asc, id asc rows between 2 preceding and current row) as last3_min_age,
        count(age) over (partition by city order by signup_date asc, id asc rows between 2 preceding and current row) as last3_count_age,
        max(age) over (partition by city order by signup_date asc, id asc rows between 2 preceding and current row) as last3_max_age,
        lead(age, 1) over (partition by city order by credit_limit asc, id asc) as next_age
    from latest_match
    where segment is not null
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from metrics s
    where exists (
            select 1
            from sessions x
            where x.customer_id = s.id
              and x.customer_id in (4, 17, 18)
        )
),
with_last as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.segment,
        s.age,
        s.last3_min_age,
        s.last3_count_age,
        s.last3_max_age,
        s.next_age,
        x.subscription_status,
        x.plan
    from kept s
    left join lateral (
            select c.status as subscription_status, c.plan
            from subscriptions c
            where c.customer_id = s.id
            order by c.end_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    segment,
    age,
    last3_min_age,
    last3_count_age,
    last3_max_age,
    next_age,
    subscription_status
from with_last
order by age asc;
