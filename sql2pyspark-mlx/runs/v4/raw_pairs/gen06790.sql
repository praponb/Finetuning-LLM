with flattened_attrs as (
    select
        id,
        plan,
        currency,
        tier_limits,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        array_min(tier_prices) as lowest_price
    from price_tiers
    where plan in ('enterprise', 'pro', 'basic')
)
select
    case when lowest_price >= 28 then 'LARGE' else 'SMALL' end as lowest_price_tier,
    round(avg(lowest_price), 2) as avg_lowest_price,
    round(avg(lowest_price), 2) as avg_lowest_price_2,
    count(case when plan = 'pro' then 1 end) as pro_count,
    max(lowest_price) as max_lowest_price,
    sum(case when plan = 'starter' then lowest_price else 0 end) as starter_lowest_price,
    max(lowest_price) filter (where plan = 'basic') as basic_max_lowest_price
from flattened_attrs
group by case when lowest_price >= 28 then 'LARGE' else 'SMALL' end
order by avg_lowest_price_2 asc nulls last;
