with deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        plan,
        start_date
    from subscriptions
    qualify row_number() over (partition by customer_id order by end_date desc nulls last, id) = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        datediff(current_date(), start_date) as days_since_start_date,
        initcap(trim(plan)) as plan_title
    from deduped
    where start_date >= date '2023-03-11'
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by plan order by start_date desc nulls last, id) as row_num
    from valid
    qualify row_num = 1
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        plan,
        start_date,
        days_since_start_date,
        plan_title
    from (
        select *, row_number() over (partition by plan order by days_since_start_date desc nulls last, id) as rank_in_group
        from ranked
    ) t
    where rank_in_group = 1
),
base as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        days_since_start_date,
        plan_title,
        case when plan = 'standard' then 1 else 0 end as is_standard
    from latest
    where start_date < '2024-04-17'
)
select
    id,
    plan,
    days_since_start_date,
    plan_title,
    is_standard
from base;
