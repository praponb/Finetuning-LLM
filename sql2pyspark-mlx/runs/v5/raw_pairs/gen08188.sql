with exploded as (
    select
        id,
        plan,
        currency,
        tier_value
    from price_tiers
    lateral view explode(tier_limits) e as tier_value
),
scored as (
    select
        *,
        cume_dist() over (partition by plan order by tier_value desc nulls last) as tier_value_cume_dist,
        percent_rank() over w_grp as tier_value_percent_rank
    from exploded
    where currency in ('EUR', 'THB')
    window w_grp as (partition by currency order by tier_value desc)
),
filtered as (
    select
        id,
        plan,
        currency,
        tier_value,
        tier_value_cume_dist,
        tier_value_percent_rank,
        tier_value_percent_rank / nullif(tier_value, 0) as tier_value_percent_rank_per_tier_value
    from scored
    where tier_value_percent_rank < 0.66
),
enriched as (
    select
        plan,
        currency,
        tier_value_cume_dist,
        cume_dist() over (partition by plan order by tier_value_percent_rank_per_tier_value desc nulls last) as tier_value_percent_rank_per_tier_value_cume_dist
    from filtered
)
select
    plan,
    tier_value_cume_dist,
    tier_value_percent_rank_per_tier_value_cume_dist
from enriched
order by tier_value_cume_dist desc
