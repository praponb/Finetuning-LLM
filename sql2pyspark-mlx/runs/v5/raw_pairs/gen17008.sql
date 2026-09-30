with unpacked as (
    -- Higher-order array functions
    select
        id,
        effective_from,
        currency,
        tier_prices,
        slice(tier_limits, 1, 2) as first_two_limits,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices
    from price_tiers
    where currency in ('THB', 'USD')
)
-- Multi-level aggregation
select
    currency,
    date_trunc('month', effective_from) as activity_month,
    count(*) as num_rows,
    count(*) as row_count,
    sort_array(collect_set(currency)) as currency_list,
    count(case when currency = 'THB' then 1 end) as thb_count,
    grouping_id() as grouping_id
from unpacked
group by cube (currency, date_trunc('month', effective_from));
