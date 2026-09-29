with flattened as (
    select
        id,
        plan,
        effective_from,
        currency,
        tier_value
    from price_tiers
    lateral view explode(discounts) t as tier_value
)
select
    id,
    currency,
    tier_value,
    count(tier_value) over (partition by currency) as currency_count_tier_value,
    dense_rank() over (order by tier_value) as tier_value_dense_rank,
    max(tier_value) over (partition by plan) as plan_max_tier_value
from flattened;
