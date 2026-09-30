with remaining_keys as (
    -- Set intersection on keys
    select channel
    from promotions
    intersect
    select channel
    from promotions
    where promo_code = 'VIP'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from promotions s
    join remaining_keys k on s.channel = k.channel
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, lower(promo_code) as promo_code_lower
    from verified
    where channel is not null
      and starts_on > date '2023-07-28'
),
enriched as (
    -- Running and moving aggregates
    select
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_lower,
        max(discount_pct) over (partition by channel) as channel_max_discount_pct,
        cume_dist() over (partition by promo_code order by discount_pct desc) as discount_pct_cume_dist,
        sum(discount_pct) over (partition by promo_code) as promo_code_sum_discount_pct,
        cume_dist() over (order by discount_pct desc) as discount_pct_cume_dist_2,
        round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share
    from filtered
    where promo_code in ('VIP', 'BUNDLE')
),
filtered_promotions as (
    -- Basic cleansing and defaults
    select
        starts_on,
        ends_on,
        promo_code_lower,
        channel_max_discount_pct,
        discount_pct_cume_dist_2,
        promo_code_sum_discount_pct - channel_max_discount_pct as promo_code_sum_discount_pct_minus_channel_max_discount_pct
    from enriched
    where discount_pct <= 0.28
)
select
    starts_on,
    promo_code_lower,
    channel_max_discount_pct
from filtered_promotions
order by channel_max_discount_pct desc
