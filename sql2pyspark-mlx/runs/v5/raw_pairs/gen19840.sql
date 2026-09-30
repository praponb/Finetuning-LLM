with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        case when city = 'Singapore' then 1 else 0 end as is_singapore
    from customers
    where signup_date >= date '2023-07-25'
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by city, country order by is_singapore desc nulls first, id) as dedup_rank
    from cleaned
    qualify dedup_rank = 1
),
latest_match as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.country,
        s.credit_limit,
        s.age,
        s.dedup_rank,
        x.subscription_start_date,
        x.subscription_monthly_fee
    from ranked s
    left join lateral (
            select c.start_date as subscription_start_date, c.monthly_fee as subscription_monthly_fee
            from subscriptions c
            where c.customer_id = s.id
            order by c.end_date desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.started_at >= s.subscription_start_date
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.opened_date <= s.subscription_start_date
        )
),
base as (
    -- Keep recent, valid rows
    select
        id,
        email,
        country,
        age,
        dedup_rank,
        subscription_start_date,
        upper(country) as country_upper
    from not_flagged
    where subscription_start_date between '2023-07-01' and '2024-12-01'
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by country order by subscription_start_date desc nulls first, id) as row_num
    from base
    qualify row_num = 1
),
stats as (
    -- Aggregate with conditional metrics
    select
        date_trunc('year', subscription_start_date) as year_start,
        country,
        case when age >= 59 then 'LARGE' else 'SMALL' end as age_tier,
        count(case when country = 'JP' then 1 end) as jp_count,
        approx_percentile(age, 0.5) as age_p50
    from latest
    group by date_trunc('year', subscription_start_date), country, case when age >= 59 then 'LARGE' else 'SMALL' end
)
select
    year_start,
    country,
    age_tier,
    jp_count,
    age_p50
from stats
order by country desc nulls last
