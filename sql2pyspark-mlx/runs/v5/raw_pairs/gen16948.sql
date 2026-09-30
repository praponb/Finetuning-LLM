with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        year(starts_on) as starts_on_year,
        coalesce(channel, 'UNKNOWN') as resolved_channel
    from promotions
    where starts_on > date '2023-08-14'
      and channel is not null
),
attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from filtered a
    left join promotions pr
        on a.channel = pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
),
filtered_promotions as (
    -- Keep recent, valid rows
    select
        id,
        promo_code,
        ends_on,
        discount_pct,
        starts_on_year,
        resolved_channel,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        starts_on_year - promo_id as starts_on_year_minus_promo_id,
        coalesce(channel, 'OTHER') as channel_filled
    from attributed
    where channel is not null
      and ends_on >= date '2023-12-13'
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        ends_on,
        discount_pct,
        starts_on_year,
        resolved_channel,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        starts_on_year_minus_promo_id,
        channel_filled,
        cast(starts_on_year_minus_promo_id as int) as starts_on_year_minus_promo_id_int
    from filtered_promotions
    where promotion_promo_code is not null
      and promo_id <= 8
)
-- Multi-level aggregation
select
    promo_id,
    date_trunc('week', ends_on) as week_start,
    promotion_promo_code,
    count_if(promotion_promo_code = 'BUNDLE') as bundle_events,
    count(case when promotion_promo_code = 'WELCOME5' then 1 end) as welcome5_count,
    count(*) filter (where promotion_promo_code = 'WELCOME5') as welcome5_rows
from valid
group by promo_id, date_trunc('week', ends_on), promotion_promo_code;
