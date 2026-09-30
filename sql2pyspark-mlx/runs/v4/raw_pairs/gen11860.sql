with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        val
    from price_tiers
    lateral view explode(discounts) x as val
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        max(val) over w_part as plan_max_val,
        avg(val) over w_part as plan_avg_val
    from lines
    window w_part as (partition by plan)
),
remaining_keys as (
    -- Set difference on keys
    select plan
    from with_windows
    except
    select plan
    from price_tiers
    where currency = 'EUR'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join remaining_keys k on s.plan = k.plan
),
remaining_keys_2 as (
    -- Set intersection on keys
    select id, plan
    from kept_rows
    intersect
    select id, plan
    from price_tiers
    where currency in ('THB', 'EUR')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    inner join remaining_keys_2 k on s.id = k.id and s.plan = k.plan
)
select
    plan,
    val,
    plan_max_val,
    plan_avg_val
from verified
order by plan_avg_val desc;
