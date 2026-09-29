with ranked as (
    -- QUALIFY-style window filter
    select *
    from subscriptions
    qualify row_number() over (partition by status, customer_id order by monthly_fee desc nulls first, id asc) = 1
),
enriched as (
    -- Attach customers attributes
    select a.*, c.name
    from ranked a
    inner join customers c
        on a.customer_id = c.id
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by status order by end_date asc nulls last, id asc) as latest_rank
    from enriched
    qualify latest_rank = 1
)
select
    id,
    customer_id,
    plan,
    end_date,
    monthly_fee,
    status,
    name,
    latest_rank,
    case when name rlike '^(Noah Park|FRANK MILLER)$' then 'GROUP_A' when name rlike '^(Alice Wong|bob smith)$' then 'GROUP_B' else 'OTHER_ZONE' end as name_zone
from latest;
