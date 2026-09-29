with flattened_attrs as (
    select
        id,
        plan,
        currency,
        tier_limits,
        tier_prices,
        aggregate(tier_prices, cast(0 as double), (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        slice(tier_limits, 1, 2) as first_two_limits,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        array_min(tier_prices) as lowest_price
    from price_tiers
)
select
    case when lowest_price >= 36 then 'HIGH' else 'LOW' end as lowest_price_tier,
    max(lowest_price) as max_lowest_price,
    sum(case when plan = 'pro' then lowest_price else 0 end) as pro_lowest_price,
    round(avg(avg_tier_price), 2) as avg_avg_tier_price,
    min(lowest_price) as min_lowest_price
from flattened_attrs
group by lowest_price_tier
