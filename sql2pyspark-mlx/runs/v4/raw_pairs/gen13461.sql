with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        tier_value
    from price_tiers
    lateral view outer explode(discounts) t as tier_value
)
-- Window metrics over several different partitions
select
    *,
    percent_rank() over (partition by plan, currency order by tier_value asc) as tier_value_percent_rank,
    max(tier_value) over (partition by plan, currency) as plan_max_tier_value,
    cume_dist() over (partition by currency order by tier_value desc) as tier_value_cume_dist,
    cume_dist() over (partition by plan, currency order by tier_value desc) as tier_value_cume_dist_2,
    rank() over (partition by currency, plan order by tier_value desc) as tier_value_rank
from exploded;
