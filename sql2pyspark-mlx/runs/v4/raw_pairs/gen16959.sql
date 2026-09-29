with flattened_attrs as (
    select
        id,
        currency,
        tier_limits,
        discounts,
        aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        slice(tier_limits, 1, 2) as first_two_limits,
        element_at(tier_prices, 1) as entry_price
    from price_tiers
)
select
    case when avg_tier_price >= 34 then 'PREMIUM' else 'STANDARD' end as avg_tier_price_tier,
    currency,
    count(case when currency = 'THB' then 1 end) as thb_count,
    approx_percentile(avg_tier_price, 0.95) as avg_tier_price_p95,
    count(*) as event_count,
    count(distinct id) as distinct_ids,
    count(distinct currency) as distinct_currencies,
    grouping(case when avg_tier_price >= 34 then 'PREMIUM' else 'STANDARD' end) as gid
from flattened_attrs
group by rollup (case when avg_tier_price >= 34 then 'PREMIUM' else 'STANDARD' end, currency)
order by thb_count nulls last
