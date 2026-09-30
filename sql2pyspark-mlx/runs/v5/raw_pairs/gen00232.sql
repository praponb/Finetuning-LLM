with exploded as (
    select
        id,
        effective_from,
        currency,
        tier_value
    from price_tiers
    lateral view explode(discounts) e as tier_value
),
filtered as (
    select *, floor(tier_value) as tier_value_whole
    from exploded
    where effective_from <= '2024-03-01'
),
filtered_price_tiers as (
    select
        effective_from,
        currency,
        tier_value_whole,
        tier_value + tier_value_whole as tier_value_plus_tier_value_whole
    from filtered
    where tier_value < 41
),
valid as (
    select *, substring(currency, 1, 2) as currency_prefix
    from filtered_price_tiers
    where effective_from > date '2024-03-23'
      and tier_value_plus_tier_value_whole < 64
)
select
    case when tier_value_whole >= 89 then 'PREMIUM' else 'STANDARD' end as tier_value_whole_tier,
    size(collect_set(currency)) as num_currencies,
    count(distinct currency) as distinct_currencies,
    count(*) as num_rows,
    percentile_approx(tier_value_plus_tier_value_whole, array(0.25, 0.5, 0.75)) as tier_value_plus_tier_value_whole_quartiles,
    sum(case when currency = 'EUR' then tier_value_whole else 0 end) as eur_tier_value_whole
from valid
group by case when tier_value_whole >= 89 then 'PREMIUM' else 'STANDARD' end
having eur_tier_value_whole > 0
order by tier_value_whole_tier;
