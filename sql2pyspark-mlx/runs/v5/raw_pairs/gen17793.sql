with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        substring(name, 1, 3) as name_prefix,
        substring(segment, 1, 2) as segment_prefix,
        coalesce(segment, 'DIRECT') as segment_filled
    from customers
    where signup_date > date '2023-05-01'
      and signup_date < '2024-05-01'
),
kept_keys as (
    -- Set intersection on keys
    select segment, country
    from cleaned
    intersect all
    select segment, country
    from customers
    where is_active = true
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.segment = k.segment and s.country = k.country
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept_rows s
    where not exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.customer_id = 15
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from subscriptions x
            where x.customer_id = s.id
              and x.customer_id = 33
        )
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        email,
        city,
        segment,
        signup_date,
        is_active,
        age,
        name_prefix,
        segment_prefix,
        initcap(trim(city)) as city_title,
        initcap(trim(city)) as city_title_2,
        coalesce(segment, 'N/A') as segment_filled_2
    from not_flagged
    where city is not null
),
filtered as (
    -- Keep recent, valid rows
    select
        city,
        segment,
        signup_date,
        age,
        city_title_2,
        segment_filled_2,
        initcap(trim(segment)) as segment_title,
        case when city in ('Singapore', 'Bangkok') then 'Priority' else 'Other' end as city_group,
        coalesce(city, 'OTHER') as resolved_city
    from valid
    where signup_date > date '2023-05-15'
),
filtered_customers as (
    -- Keep recent, valid rows
    select *, add_months(signup_date, 6) as signup_date_plus_months
    from filtered
    where signup_date >= date '2023-08-17'
      and city is not null
)
-- Multi-level aggregation
select
    date_trunc('week', signup_date) as week_start,
    case when age >= 73 then 'HIGH' else 'LOW' end as age_tier,
    segment,
    round(avg(age), 2) as avg_age,
    round(avg(age), 2) as avg_age_2,
    count(*) filter (where city = 'Bangkok') as bangkok_rows,
    count(case when city = 'Berlin' then 1 end) as berlin_count
from filtered_customers
group by week_start, age_tier, segment
having bangkok_rows >= 1
order by bangkok_rows desc
