with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        tier_value
    from price_tiers
    lateral view outer explode(discounts) e as tier_value
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, day(effective_from) as effective_from_day
    from exploded
    where effective_from >= '2023-06-01'
),
filtered_price_tiers as (
    -- Basic cleansing and defaults
    select *, case when effective_from_day >= 87 then 'High' when effective_from_day >= 31 then 'Medium' else 'Low' end as effective_from_day_tier
    from filtered
    where effective_from > to_date('2024-02-26')
      and effective_from_day = 70
),
enriched as (
    -- Ranking and gap features
    select
        id,
        effective_from,
        tier_value,
        effective_from_day,
        effective_from_day_tier,
        cume_dist() over (order by effective_from_day desc nulls first) as effective_from_day_cume_dist,
        rank() over (order by tier_value desc) as tier_value_rank,
        rank() over w_ordered as tier_value_rank_2,
        rank() over w_recent as effective_from_day_rank
    from filtered_price_tiers
    window w_ordered as (order by tier_value asc nulls first), w_recent as (order by effective_from_day desc)
)
select
    id,
    effective_from,
    tier_value,
    effective_from_day,
    effective_from_day_tier,
    tier_value_rank,
    effective_from_day_rank
from enriched;
