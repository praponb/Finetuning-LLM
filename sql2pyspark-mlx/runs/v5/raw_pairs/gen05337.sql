with with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions s
    left join promotions pr
        on s.channel = pr.channel
       and s.starts_on >= pr.starts_on
       and s.starts_on < pr.ends_on + interval 14 days
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from with_window
    qualify row_number() over (partition by promo_code, promotion_promo_code order by promo_id desc, id asc, promo_id asc) = 1
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(promotion_discount_pct, 1) as promotion_discount_pct_rounded,
        coalesce(promotion_promo_code, 'NONE') as resolved_promotion_promo_code
    from ranked
    where ends_on > date '2023-10-08'
),
with_window_promotions as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from cleaned e
    left join promotions pr
        on e.channel = pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on < pr.ends_on + interval '7' day
),
cleaned_promotions as (
    -- Keep recent, valid rows
    select
        id,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        promotion_discount_pct_rounded,
        resolved_promotion_promo_code,
        promo_id_2,
        promotion_promo_code_2,
        promotion_discount_pct_2,
        case when promotion_discount_pct_2 >= 0.3 then 'High' when promotion_discount_pct_2 >= 0.12 then 'Medium' else 'Low' end as promotion_discount_pct_2_tier,
        coalesce(promotion_promo_code_2, 'NONE') as promotion_promo_code_2_filled
    from with_window_promotions
    where promo_id_2 > 7
)
-- Grouped metrics
select
    promo_id,
    sum(case when promotion_promo_code_2 = 'BUNDLE' then promotion_discount_pct_rounded else 0 end) as bundle_promotion_discount_pct_rounded,
    round(sum(promotion_discount_pct_2) / nullif(count(case when promotion_promo_code_2 = 'BUNDLE' then 1 end), 0), 2) as promotion_discount_pct_2_per_bundle,
    round(sum(promotion_discount_pct) / nullif(count(case when promotion_promo_code_2 = 'WELCOME5' then 1 end), 0), 2) as promotion_discount_pct_per_welcome5,
    sum(promotion_discount_pct_2) filter (where promotion_promo_code_2 = 'VIP') as vip_total_promotion_discount_pct_2
from cleaned_promotions
group by promo_id
order by bundle_promotion_discount_pct_rounded asc;
