with parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        plan,
        effective_from,
        currency,
        discounts,
        element_at(tier_prices, 1) as entry_price,
        slice(tier_limits, 1, 2) as first_two_limits,
        aggregate(tier_prices, cast(0 as double), (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps
    from price_tiers
)
-- Aggregate with conditional metrics
select
    currency,
    date_trunc('month', effective_from) as activity_month,
    case when entry_price >= 30 then 'LARGE' else 'SMALL' end as entry_price_tier,
    round(avg(avg_tier_price), 2) as avg_avg_tier_price,
    approx_percentile(entry_price, array(0.25, 0.5, 0.75)) as entry_price_quartiles,
    max(avg_tier_price) filter (where currency = 'EUR') as eur_max_avg_tier_price,
    count(distinct currency) as distinct_currencies,
    size(collect_set(currency)) as num_currencies,
    percent_rank() over (partition by currency order by round(avg(avg_tier_price), 2) desc) as avg_avg_tier_price_percent_rank
from parsed
group by currency, date_trunc('month', effective_from), case when entry_price >= 30 then 'LARGE' else 'SMALL' end
having round(avg(avg_tier_price), 2) > 0
