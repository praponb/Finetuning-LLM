with parsed as (
    select
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices
    from price_tiers
)
select
    date_trunc('week', effective_from) as week_start,
    size(collect_set(currency)) as num_currencies,
    count(*) as num_rows,
    count_if(currency = 'THB') as thb_events
from parsed
group by date_trunc('week', effective_from)
order by num_currencies desc;
