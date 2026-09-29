with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(is_active as int) as is_active_flag,
        length(country) as country_length
    from customers
    where year(signup_date) = 2024
),
matched_keys as (
    -- Set intersection on keys
    select city
    from filtered
    intersect
    select city
    from customers
    where credit_limit between 4400 and 7200
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.city = k.city
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        greatest(is_active_flag, age) as greatest_is_active_flag_age,
        coalesce(city, 'DIRECT') as resolved_city
    from verified
    where country_length > 24
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.status in ('OPEN', 'CLOSED')
        )
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from not_flagged s
    where exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.opened_date < s.signup_date
        )
)
select
    id,
    name,
    country,
    city,
    segment,
    is_active,
    credit_limit,
    age,
    is_active_flag,
    case when city rlike '^(New York|Berlin)$' then 'PRIORITY' when city rlike '^(Bangkok|Chiang Mai)$' then 'EXTENDED' else 'CORE' end as city_zone
from eligible
order by city desc nulls last
