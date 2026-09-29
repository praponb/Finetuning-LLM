with base as (
    -- Filter the raw rows and derive helper columns
    select *, initcap(trim(name)) as name_title
    from customers
    where city is not null
      and city is not null
),
remaining_keys as (
    -- Set difference on keys
    select city
    from base
    except
    select city
    from customers
    where credit_limit is not null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join remaining_keys k on s.city = k.city
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.opened_date < s.signup_date
        )
),
kept_customers as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from sessions x
            where x.customer_id = s.id
              and x.device != 'desktop'
        )
)
-- Multi-level aggregation
select
    case when credit_limit >= 15300 then 'LARGE' else 'SMALL' end as credit_limit_tier,
    case when age >= 70 then 'PREMIUM' else 'STANDARD' end as age_tier,
    stddev_samp(age) as age_stddev,
    min(age) as min_age,
    count(distinct country) as distinct_countries,
    round(avg(credit_limit), 2) as avg_credit_limit,
    count(*) filter (where country = 'DE') as de_rows
from kept_customers
group by credit_limit_tier, age_tier
having stddev_samp(age) < 1000000
order by distinct_countries nulls last
