with flattened_attrs as (
    select
        id,
        plan,
        effective_from,
        tier_limits,
        element_at(tier_prices, 1) as entry_price,
        slice(tier_limits, 1, 2) as first_two_limits,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        array_min(tier_prices) as lowest_price
    from price_tiers
    where effective_from > '2023-11-01'
)
select
    id,
    plan,
    effective_from,
    tier_limits,
    entry_price,
    first_two_limits,
    tier_revenue_caps,
    avg_tier_price,
    lowest_price,
    sum(avg_tier_price) over (order by entry_price, id rows between unbounded preceding and current row) as cumulative_avg_tier_price,
    min(avg_tier_price) over (partition by plan) as plan_min_avg_tier_price,
    stddev_samp(lowest_price) over (partition by plan) as plan_stddev_samp_lowest_price,
    percent_rank() over (partition by plan order by lowest_price desc) as lowest_price_percent_rank
from flattened_attrs
