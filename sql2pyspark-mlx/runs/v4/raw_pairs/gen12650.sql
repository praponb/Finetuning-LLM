with flattened_attrs as (
    select
        id,
        plan,
        currency,
        tier_limits,
        array_min(tier_prices) as lowest_price,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices
    from price_tiers
)
select
    *,
    max(lowest_price) over (partition by currency, plan) as currency_max_lowest_price,
    sum(lowest_price) over (partition by plan) as plan_sum_lowest_price,
    lag(lowest_price, 1) over (order by lowest_price, id) as prev_lowest_price,
    avg(lowest_price) over (partition by currency order by lowest_price, id rows between 5 preceding and 1 preceding) as lowest_price_avg_5_prior,
    ntile(5) over (partition by plan, currency order by lowest_price desc, id) as lowest_price_bucket
from flattened_attrs
