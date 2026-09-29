with base as (
    -- Filter the raw rows and derive helper columns
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
        if(is_active, 'Yes', 'No') as is_active_label,
        lower(segment) as segment_lower
    from customers
    where length(name) > 12
),
latest as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by segment order by age) as dedup_rank
    from base
    qualify dedup_rank = 1
),
with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, x.order_status
    from latest s
    join lateral (
            select c.status as order_status
            from orders c
            where c.customer_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.order_date < s.signup_date
        )
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by city, order_status order by age desc) as row_num
        from eligible
    ) t
    where row_num = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by city, order_status order by credit_limit nulls first, id) as rn
    from ranked
    qualify rn = 1
)
select
    id,
    name,
    email,
    segment,
    is_active,
    credit_limit,
    age,
    segment_lower,
    dedup_rank,
    case when name rlike '^(  Carol Diaz|David Lee )$' then 'GROUP_B' else 'CORE' end as name_zone
from deduped
order by credit_limit;
