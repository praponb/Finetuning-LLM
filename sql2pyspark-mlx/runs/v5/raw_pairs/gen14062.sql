with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        inline(items)
    from carts
),
flattened as (
    -- Unnest the array into one row per element
    select
        id,
        price,
        explode_outer(coupon_codes) as code
    from exploded
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        lower(code) as code_lower,
        round(price * 1.07, 2) as adjusted_price,
        coalesce(code, 'OTHER') as code_filled
    from flattened
    where code is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        code,
        adjusted_price,
        adjusted_price + price as adjusted_price_plus_price,
        least(price, adjusted_price) as least_price_adjusted_price
    from cleaned
    where price >= 150
)
-- Aggregate with conditional metrics
select
    case when adjusted_price >= 84 then 'LARGE' else 'SMALL' end as adjusted_price_tier,
    size(collect_set(code)) as num_codes,
    count(case when code = 'VIP' then 1 end) as vip_count,
    round(sum(adjusted_price) / nullif(count(case when code = 'WELCOME5' then 1 end), 0), 2) as adjusted_price_per_welcome5,
    max(least_price_adjusted_price) filter (where code = 'VIP') as vip_max_least_price_adjusted_price
from filtered
group by case when adjusted_price >= 84 then 'LARGE' else 'SMALL' end
order by adjusted_price_tier
