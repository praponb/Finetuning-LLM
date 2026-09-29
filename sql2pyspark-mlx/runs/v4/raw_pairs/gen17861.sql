with base as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        case when discount_pct >= 0.26 then 'High' when discount_pct >= 0.13 then 'Medium' else 'Low' end as discount_pct_level,
        case when discount_pct >= 0.27 then 'High' when discount_pct >= 0.16 then 'Medium' else 'Low' end as discount_pct_band
    from promotions
    where ends_on >= date '2024-01-15'
      and channel is not null
      and month(starts_on) in (5, 6, 11)
),
base_promotions as (
    select
        id,
        promo_code,
        ends_on,
        discount_pct,
        discount_pct_level,
        date_format(starts_on, 'MMM yyyy') as starts_on_label,
        add_months(starts_on, 1) as starts_on_plus_months
    from base
    where starts_on >= date '2023-11-20'
      and channel is not null
      and channel = 'mobile'
),
with_windows as (
    select
        id,
        promo_code,
        ends_on,
        discount_pct,
        discount_pct_level,
        starts_on_label,
        starts_on_plus_months,
        ntile(4) over (partition by promo_code order by discount_pct, id) as discount_pct_bucket,
        lag(discount_pct, 1) over (order by discount_pct, id) as prev_discount_pct,
        round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share,
        row_number() over (order by discount_pct desc, id) as row_seq,
        round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share_2
    from base_promotions
),
with_windows_promotions as (
    select
        *,
        lag(discount_pct, 1) over (partition by promo_code order by discount_pct, id) as prev_discount_pct_2,
        max(discount_pct_share) over (partition by promo_code) as promo_code_max_discount_pct_share,
        lag(discount_pct_share_2, 2) over (order by discount_pct, id) as prev_discount_pct_share_2
    from with_windows
),
deduped as (
    select *
    from with_windows_promotions
    qualify row_number() over (partition by promo_code order by ends_on desc, id) = 1
)
select
    id,
    promo_code,
    ends_on,
    discount_pct,
    starts_on_label,
    discount_pct_bucket,
    prev_discount_pct,
    row_seq,
    prev_discount_pct_2
from deduped;
