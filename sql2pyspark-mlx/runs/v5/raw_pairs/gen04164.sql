with base as (
    -- Keep recent, valid rows
    select
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        concat(name, '/', city) as name_city
    from customers
    where signup_date > to_date('2023-05-04')
),
base_customers as (
    -- Keep recent, valid rows
    select
        id,
        name,
        email,
        segment,
        is_active,
        credit_limit,
        age,
        lower(email) as email_lower
    from base
    where age <= 38
      and signup_date > to_date('2023-07-19')
),
latest_match as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select s.*, latest.opened_date
    from base_customers s
    left join lateral (
            select c.opened_date
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) latest on true
),
kept_keys as (
    -- Set intersection on keys
    select id, segment
    from latest_match
    intersect all
    select id, segment
    from customers
    where name like 'Oli%'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest_match s
    inner join kept_keys k on s.id = k.id and s.segment = k.segment
),
cleaned as (
    -- Basic cleansing and defaults
    select
        email,
        segment,
        is_active,
        credit_limit,
        email_lower,
        opened_date,
        upper(segment) as segment_upper
    from kept_rows
    where is_active = true
)
-- Multi-level aggregation
select
    case when credit_limit >= 17500 then 'PREMIUM' else 'STANDARD' end as credit_limit_tier,
    count(case when segment = 'SMB' then 1 end) as smb_count,
    avg(credit_limit) filter (where segment = 'Corporate') as corporate_avg_credit_limit
from cleaned
group by 1
having avg(credit_limit) filter (where segment = 'Corporate') < 1000000
order by credit_limit_tier
