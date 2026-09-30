with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(city)) as city_title,
        initcap(trim(email)) as email_title,
        coalesce(segment, 'UNKNOWN') as segment_clean
    from customers
    where signup_date > date '2023-10-30'
      and is_active = false
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from subscriptions x
            where x.customer_id = s.id
              and x.end_date >= s.signup_date
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        city_title,
        email_title,
        segment_clean,
        row_number() over (order by credit_limit, id) as event_seq,
        lead(age, 1) over (partition by segment order by age, id) as next_age,
        corr(credit_limit, age) over (partition by segment) as corr_credit_limit_age,
        dense_rank() over (partition by segment order by age desc nulls first) as age_dense_rank,
        row_number() over (partition by city order by age, id) as event_seq_2
    from not_flagged
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        country,
        corr_credit_limit_age,
        credit_limit
    from enriched
) src
unpivot (
    reading for metric_name in (corr_credit_limit_age as CORR_CREDIT_LIMIT_AGE_VAL, credit_limit)
)
