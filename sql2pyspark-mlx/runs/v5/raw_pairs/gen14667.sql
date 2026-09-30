with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        tier_value
    from price_tiers
    lateral view outer explode(discounts) t as tier_value
),
kept_keys as (
    -- Set difference on keys
    select id
    from flattened
    except all
    select id
    from price_tiers
    where effective_from < '2025-10-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from flattened s
    inner join kept_keys k on s.id = k.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(plan) plan_upper,
        cast(tier_value as int) tier_value_int
    from kept_rows
    where effective_from > date '2024-03-10'
),
cleaned_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        effective_from,
        plan_upper,
        tier_value_int,
        tier_value * tier_value_int tier_value_times_tier_value_int
    from cleaned
    where effective_from > date '2024-03-06'
),
scored as (
    -- Ranking and gap features
    select *, count(tier_value_times_tier_value_int) over (partition by plan) plan_count_tier_value_times_tier_value_int
    from cleaned_price_tiers
)
-- Aggregate with conditional metrics
select
    case when plan_count_tier_value_times_tier_value_int >= 16 then 'PREMIUM' else 'STANDARD' end as plan_count_tier_value_times_tier_value_int_tier,
    plan,
    count(distinct plan) as distinct_plans,
    size(collect_set(plan)) as num_plans,
    count_if(plan = 'pro') as pro_events
from scored
group by 1, 2
having num_plans > 1
