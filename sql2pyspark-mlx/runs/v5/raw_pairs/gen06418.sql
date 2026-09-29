with parsed as (
    select
        id,
        tier_limits,
        discounts,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price
    from price_tiers
)
select
    case when avg_tier_price >= 33 then 'HIGH' else 'LOW' end as avg_tier_price_tier,
    round(avg(avg_tier_price), 2) as avg_avg_tier_price,
    count(*) as event_count,
    min(avg_tier_price) as min_avg_tier_price,
    round(avg(avg_tier_price), 2) as avg_avg_tier_price_2,
    count(distinct id) as distinct_ids,
    grouping_id() as gid
from parsed
group by rollup (case when avg_tier_price >= 33 then 'HIGH' else 'LOW' end)
order by avg_tier_price_tier desc nulls first;
