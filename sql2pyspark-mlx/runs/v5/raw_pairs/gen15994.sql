with valid as (
    -- Keep recent, valid rows
    select *, age + credit_limit as age_plus_credit_limit
    from customers
    where signup_date > to_date('2023-07-07')
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from sessions c
            where c.customer_id = s.id
              and c.country in ('JP', 'US')
        )
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by city order by age desc nulls first, id) as rank_in_group
    from not_flagged
    qualify rank_in_group = 1
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from deduped s
    where exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.browser not in ('Safari', 'Firefox', 'Edge')
        )
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        email,
        country,
        city,
        is_active,
        credit_limit,
        age_plus_credit_limit,
        rank_in_group,
        count(age_plus_credit_limit) over (partition by segment, city order by age, id rows between 3 preceding and current row) as last4_count_age_plus_credit_limit,
        sum(age_plus_credit_limit) over (partition by segment, city order by age, id rows between 3 preceding and current row) as last4_sum_age_plus_credit_limit,
        avg(age_plus_credit_limit) over (partition by segment, city order by age, id rows between 3 preceding and current row) as last4_avg_age_plus_credit_limit,
        lead(age, 1) over (partition by segment, city order by age, id) as next_age
    from eligible
),
grouped as (
    -- Multi-level aggregation
    select
        case when credit_limit >= 14500 then 'LARGE' else 'SMALL' end as credit_limit_tier,
        case when last4_sum_age_plus_credit_limit >= 410 then 'HIGH' else 'LOW' end as last4_sum_age_plus_credit_limit_tier,
        approx_percentile(next_age, 0.95) as next_age_p95,
        round(sum(last4_avg_age_plus_credit_limit) / nullif(count(case when city = 'London' then 1 end), 0), 2) as last4_avg_age_plus_credit_limit_per_london,
        count(case when city = 'New York' then 1 end) as new_york_count,
        count(*) as event_count,
        count_if(city = 'Chiang Mai') as chiang_mai_events,
        approx_percentile(credit_limit, array(0.25, 0.5, 0.75)) as credit_limit_quartiles,
        percent_rank() over (partition by case when credit_limit >= 14500 then 'LARGE' else 'SMALL' end order by count(*) desc) as event_count_percent_rank
    from metrics
    group by case when credit_limit >= 14500 then 'LARGE' else 'SMALL' end, case when last4_sum_age_plus_credit_limit >= 410 then 'HIGH' else 'LOW' end
)
select
    credit_limit_tier,
    last4_sum_age_plus_credit_limit_tier,
    last4_avg_age_plus_credit_limit_per_london,
    event_count,
    chiang_mai_events,
    credit_limit_quartiles
from grouped
order by event_count desc
