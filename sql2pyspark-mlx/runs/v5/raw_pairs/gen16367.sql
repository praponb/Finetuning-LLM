with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when city = 'Bangkok' then 1 else 0 end is_bangkok,
        age - credit_limit age_minus_credit_limit,
        coalesce(segment, 'UNKNOWN') segment_clean
    from customers
    where is_active = true
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        email,
        country,
        signup_date,
        is_active,
        credit_limit,
        age,
        is_bangkok,
        age_minus_credit_limit,
        segment_clean,
        case when city = 'Singapore' then 1 else 0 end is_singapore,
        coalesce(city, 'UNKNOWN') resolved_city
    from cleaned
    where is_active = true
),
remaining_keys as (
    -- Set difference on keys
    select country, id
    from valid
    except
    select country, id
    from customers
    where country != 'US'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    join remaining_keys k on s.country = k.country and s.id = k.id
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept_rows s
    where not exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.started_at < s.signup_date
        )
),
metrics as (
    -- Window metrics over several different partitions
    select *, sum(age) over w_part country_sum_age
    from not_flagged
    window w_part as (partition by country)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.status is not null
        )
),
base as (
    -- Basic cleansing and defaults
    select *, substring(email, 1, 3) email_prefix
    from kept
    where signup_date > date '2023-08-13'
)
-- Grouped metrics
select
    case when country_sum_age >= 273 then 'LARGE' else 'SMALL' end as country_sum_age_tier,
    country,
    size(collect_set(country)) as num_countries,
    approx_percentile(is_singapore, 0.95) as is_singapore_p95,
    bit_or(age) as age_bit_or,
    count_if(country = 'TH') as th_events,
    round(sum(is_singapore) / nullif(count(case when country = 'JP' then 1 end), 0), 2) as is_singapore_per_jp
from base
group by case when country_sum_age >= 273 then 'LARGE' else 'SMALL' end, country
having is_singapore_p95 >= 0
