with parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        slice(tier_limits, 1, 2) as first_two_limits,
        array_min(tier_prices) as lowest_price
    from price_tiers
)
-- Aggregate with conditional metrics
select
    date_trunc('month', effective_from) as activity_month,
    currency,
    count_if(currency = 'USD') as usd_events,
    percentile_approx(lowest_price, array(0.25, 0.5, 0.75)) as lowest_price_quartiles,
    approx_percentile(lowest_price, 0.95) as lowest_price_p95,
    dense_rank() over (partition by date_trunc('month', effective_from) order by approx_percentile(lowest_price, 0.95) desc) as lowest_price_p95_dense_rank
from parsed
group by date_trunc('month', effective_from), currency
order by currency;
