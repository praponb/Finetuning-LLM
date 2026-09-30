with matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions a
    inner join promotions pr
        on coalesce(a.channel, 'web') = pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
),
matched_promotions as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from matched src
    left join promotions pr
        on src.channel = pr.channel
       and src.ends_on >= pr.starts_on
       and src.ends_on <= date_add(pr.ends_on, 7)
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        round(promo_id / sum(promo_id) over (partition by promo_code), 4) as promo_id_share,
        percent_rank() over (order by discount_pct) as discount_pct_percent_rank,
        sum(promotion_discount_pct) over (partition by promotion_promo_code) as promotion_promo_code_sum_promotion_discount_pct,
        sum(promotion_discount_pct) over (order by ends_on, id, promo_id, promo_id_2 rows between unbounded preceding and current row) as running_promotion_discount_pct,
        ntile(5) over (partition by promo_code order by promo_id_2, id, promo_id, promo_id_2) as promo_id_2_bucket
    from matched_promotions
),
base as (
    -- Basic cleansing and defaults
    select *, least(running_promotion_discount_pct, promo_id_share) as least_running_promotion_discount_pct_promo_id_share
    from with_windows
    where promotion_promo_code is not null
),
attributed as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id_3,
        pr.promo_code as promotion_promo_code_3,
        pr.discount_pct as promotion_discount_pct_3
    from base s
    left join promotions pr
        on s.channel <=> pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on <= date_add(pr.ends_on, 10)
),
matched_promotions_2 as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id_4,
        pr.promo_code as promotion_promo_code_4,
        pr.discount_pct as promotion_discount_pct_4
    from attributed e
    inner join promotions pr
        on e.channel <=> pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on <= date_add(pr.ends_on, 3)
),
scored as (
    -- Ranking and gap features
    select
        *,
        max(promo_id_2) over (partition by promotion_promo_code_3) as promotion_promo_code_3_max_promo_id_2,
        first_value(promo_id_4) ignore nulls over (partition by promotion_promo_code_3, promotion_promo_code_4 order by promo_id_share, id, promo_id, promo_id_2, promo_id_3, promo_id_4 rows between unbounded preceding and current row) as first_known_promo_id_4
    from matched_promotions_2
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        promo_id,
        promo_id_2,
        promo_id_3,
        promo_id_4,
        promotion_promo_code_3,
        discount_pct_percent_rank,
        promotion_discount_pct_2
    from scored
) src
unpivot include nulls (
    reading for metric in (discount_pct_percent_rank as DISCOUNT_PCT_PERCENT_RANK, promotion_discount_pct_2 as PROMOTION_DISCOUNT_PCT_2_VAL)
)
