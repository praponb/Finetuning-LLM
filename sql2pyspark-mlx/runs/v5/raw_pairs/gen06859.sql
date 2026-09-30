with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        tier_value
    from price_tiers
    lateral view outer explode(tier_limits) t as tier_value
),
kept_keys as (
    -- Set intersection on keys
    select id
    from exploded
    intersect
    select id
    from price_tiers
    where currency = 'USD'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded s
    inner join kept_keys k on s.id = k.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        effective_from,
        tier_value,
        dense_rank() over (order by tier_value nulls first) as tier_value_dense_rank,
        percent_rank() over (order by tier_value) as tier_value_percent_rank,
        rank() over (order by tier_value desc nulls last) as tier_value_rank,
        percent_rank() over (order by tier_value desc nulls last) as tier_value_percent_rank_2,
        dense_rank() over (order by tier_value nulls last) as tier_value_dense_rank_2
    from verified
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by tier_value nulls first) as tier_value_rank_2,
        rank() over (order by tier_value_percent_rank desc nulls last) as tier_value_percent_rank_rank
    from with_windows
)
select
    effective_from,
    tier_value,
    tier_value_percent_rank,
    tier_value_rank,
    tier_value_percent_rank_2,
    tier_value_rank_2,
    tier_value_percent_rank_rank
from enriched
order by tier_value_percent_rank desc nulls last
