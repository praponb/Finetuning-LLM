with filtered as (
    -- Keep recent, valid rows
    select *, concat_ws('/', segment, email) as segment_email_key
    from customers
    where signup_date >= date '2023-05-24'
      and city is null
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from filtered
    qualify row_number() over (partition by city order by age desc nulls first, id) = 1
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from latest
    qualify row_number() over (partition by city, country order by age desc nulls last, id) = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        age + credit_limit as age_plus_credit_limit,
        coalesce(segment, 'N/A') as segment_filled
    from deduped
    where segment is not null
      and credit_limit < 14400
),
deduped_customers as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by country order by age nulls first, id) as latest_rank
    from valid
    qualify latest_rank = 1
),
grouped as (
    -- Multi-level aggregation
    select
        case when age >= 72 then 'HIGH' else 'LOW' end as age_tier,
        approx_percentile(age, 0.95) as age_p95,
        max(age) filter (where country = 'US') as us_max_age
    from deduped_customers
    group by case when age >= 72 then 'HIGH' else 'LOW' end
)
select age_tier, us_max_age
from grouped
