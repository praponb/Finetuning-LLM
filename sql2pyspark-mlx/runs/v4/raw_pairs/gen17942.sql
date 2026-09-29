with parsed as (
    -- Semi-structured parsing
    select
        id,
        tier_limits,
        discounts,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        slice(tier_limits, 1, 2) as first_two_limits,
        array_min(tier_prices) as lowest_price
    from price_tiers
)
-- Grouped metrics
select
    case when lowest_price >= 27 then 'PREMIUM' else 'STANDARD' end as lowest_price_tier,
    case when avg_tier_price >= 33 then 'LARGE' else 'SMALL' end as avg_tier_price_tier,
    approx_percentile(lowest_price, 0.95) as lowest_price_p95,
    percentile_approx(avg_tier_price, 0.9) as avg_tier_price_p90,
    approx_percentile(lowest_price, array(0.25, 0.5, 0.75)) as lowest_price_quartiles,
    count(*) as cnt,
    round(avg(avg_tier_price), 2) as avg_avg_tier_price
from parsed
group by case when lowest_price >= 27 then 'PREMIUM' else 'STANDARD' end, case when avg_tier_price >= 33 then 'LARGE' else 'SMALL' end
order by avg_tier_price_p90 desc;
