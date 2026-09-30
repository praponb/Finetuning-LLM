with flattened_attrs as (
    select
        id,
        plan,
        discounts,
        aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        slice(tier_limits, 1, 2) as first_two_limits,
        array_min(tier_prices) as lowest_price,
        element_at(tier_prices, 1) as entry_price
    from price_tiers
)
select
    id,
    plan,
    avg_tier_price,
    lowest_price,
    entry_price,
    lead(avg_tier_price) over (partition by plan order by avg_tier_price, id) as next_avg_tier_price,
    lag(avg_tier_price) over (partition by plan order by avg_tier_price, id) as prev_avg_tier_price,
    lead(entry_price) over (partition by plan order by avg_tier_price, id) as next_entry_price,
    lag(entry_price) over (partition by plan order by avg_tier_price, id) as prev_entry_price,
    row_number() over (partition by plan order by avg_tier_price, id) as event_seq
from flattened_attrs
where lowest_price between 10 and 18;
