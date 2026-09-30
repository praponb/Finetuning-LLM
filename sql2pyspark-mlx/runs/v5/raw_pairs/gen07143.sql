with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        credit_limit,
        concat_ws('/', segment, name) as segment_name_key,
        substring(city, 1, 2) as city_prefix
    from customers
    where signup_date > to_date('2023-03-09')
      and segment is distinct from 'Consumer'
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by city, segment order by credit_limit nulls first, id) as row_num
    from base
    qualify row_num = 1
),
deduped_customers as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        name,
        email,
        credit_limit,
        segment_name_key,
        city_prefix,
        row_num
    from deduped
    qualify row_number() over (partition by country order by credit_limit nulls last, id) = 1
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from deduped_customers s
    where not exists (
            select 1
            from subscriptions x
            where x.customer_id = s.id
              and x.status = 'CANCELLED'
        )
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept s
    where exists (
            select 1
            from sessions c
            where c.customer_id = s.id
              and c.browser not in ('Edge', 'Chrome', 'Safari')
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(credit_limit) over (order by row_num, id rows between unbounded preceding and current row) as running_credit_limit,
        first_value(credit_limit, true) over (order by credit_limit, id rows between unbounded preceding and current row) as first_known_credit_limit
    from not_flagged
),
scored as (
    -- Running and moving aggregates
    select
        id,
        email,
        credit_limit,
        running_credit_limit,
        first_known_credit_limit,
        lag(credit_limit, 2) over (order by row_num, id) as prev_credit_limit,
        lead(running_credit_limit, 1) over (order by first_known_credit_limit, id) as next_running_credit_limit
    from enriched
)
-- Running and moving aggregates
select
    *,
    sum(credit_limit) over w_recent as cumulative_credit_limit,
    last_value(credit_limit, true) over w_recent as last_known_credit_limit
from scored
where first_known_credit_limit >= 13800
window w_recent as (order by prev_credit_limit, id rows between unbounded preceding and current row)
