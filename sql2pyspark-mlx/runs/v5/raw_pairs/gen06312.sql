with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        name,
        email,
        city,
        segment,
        is_active,
        age,
        case when country = 'US' then 1 else 0 end as is_us,
        coalesce(segment, 'N/A') as segment_clean
    from customers
    where signup_date >= date '2023-04-18'
),
metrics as (
    -- Ranking and gap features
    select
        id,
        name,
        email,
        is_active,
        age,
        is_us,
        avg(is_us) over (partition by city order by age, id rows between 5 preceding and 1 preceding) as is_us_avg_5_prior,
        max(is_us) over (partition by segment) as segment_max_is_us
    from valid
    where is_active
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(age) over (order by is_us, id rows between unbounded preceding and current row) as cumulative_age,
        ntile(10) over (order by is_us, id) as is_us_bucket
    from metrics
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from subscriptions c
            where c.customer_id = s.id
              and c.status not in ('ACTIVE', 'CANCELLED', 'EXPIRED')
        )
),
eligible_customers as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.country in ('US', 'JP')
        )
)
-- Grouped metrics
select
    case when is_us_bucket >= 7 then 'PREMIUM' else 'STANDARD' end as is_us_bucket_tier,
    case when is_us_avg_5_prior >= 75 then 'PREMIUM' else 'STANDARD' end as is_us_avg_5_prior_tier,
    case when age >= 72 then 'LARGE' else 'SMALL' end as age_tier,
    round(avg(segment_max_is_us), 2) as avg_segment_max_is_us,
    avg(is_us) as avg_is_us,
    count(*) as row_count,
    bit_or(age) as age_bit_or,
    count(*) as num_rows
from eligible_customers
group by case when is_us_bucket >= 7 then 'PREMIUM' else 'STANDARD' end, case when is_us_avg_5_prior >= 75 then 'PREMIUM' else 'STANDARD' end, case when age >= 72 then 'LARGE' else 'SMALL' end
order by age_bit_or;
