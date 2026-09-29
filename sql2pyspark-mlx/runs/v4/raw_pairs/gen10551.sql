with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        substring(city, 1, 2) as city_prefix,
        date_add(signup_date, 43) as signup_date_plus_43d,
        coalesce(segment, 'N/A') as segment_filled
    from customers
    where signup_date > date '2024-03-10'
      and city is not null
),
kept_keys as (
    -- Set intersection on keys
    select id, segment
    from cleaned
    intersect
    select id, segment
    from customers
    where credit_limit >= 6700
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    join kept_keys k on s.id = k.id and s.segment = k.segment
),
scored as (
    -- Running and moving aggregates
    select
        *,
        round(age / sum(age) over (partition by segment), 4) as age_share,
        corr(credit_limit, age) over w_ordered as corr_credit_limit_age,
        cume_dist() over w_part as credit_limit_cume_dist,
        avg(age) over w_ordered as segment_avg_age
    from verified
    window w_ordered as (partition by segment), w_part as (partition by segment order by credit_limit desc)
)
-- Running and moving aggregates
select
    id,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    city_prefix,
    signup_date_plus_43d,
    segment_filled,
    age_share,
    corr_credit_limit_age,
    credit_limit_cume_dist,
    segment_avg_age,
    rank() over (order by corr_credit_limit_age nulls first) as corr_credit_limit_age_rank,
    avg(credit_limit_cume_dist) over (partition by segment) as segment_avg_credit_limit_cume_dist
from scored
