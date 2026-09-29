with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        tier_value
    from price_tiers
    lateral view outer explode(tier_limits) t as tier_value
),
filtered as (
    -- Basic cleansing and defaults
    select *, case when currency = 'EUR' then 1 else 0 end as is_eur
    from exploded
    where tier_value <= 76
),
scored as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over w_hist as tier_value_dense_rank,
        dense_rank() over w_hist_2 as is_eur_dense_rank,
        cume_dist() over (partition by currency order by tier_value desc) as tier_value_cume_dist,
        cume_dist() over w_recent as tier_value_cume_dist_2,
        stddev_samp(tier_value) over (partition by currency) as currency_stddev_samp_tier_value
    from filtered
    window w_hist as (order by tier_value desc), w_hist_2 as (order by is_eur desc nulls last), w_recent as (order by tier_value desc nulls last)
),
base as (
    -- Basic cleansing and defaults
    select *, round(tier_value_dense_rank * 1.07, 2) as adjusted_tier_value_dense_rank
    from scored
    where effective_from >= to_date('2024-02-08')
),
cleaned as (
    -- Basic cleansing and defaults
    select *, tier_value_dense_rank * tier_value as tier_value_dense_rank_times_tier_value
    from base
    where effective_from >= date '2024-02-25'
),
base_price_tiers as (
    -- Basic cleansing and defaults
    select *, initcap(trim(currency)) as currency_title
    from cleaned
    where tier_value_cume_dist_2 < 0.7
)
select
    effective_from,
    tier_value,
    is_eur,
    tier_value_dense_rank,
    is_eur_dense_rank,
    tier_value_cume_dist,
    tier_value_cume_dist_2,
    adjusted_tier_value_dense_rank,
    currency_title
from base_price_tiers;
