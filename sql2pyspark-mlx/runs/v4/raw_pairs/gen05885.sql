with valid as (
    -- Keep recent, valid rows
    select *, substring(promo_code, 1, 2) as promo_code_prefix
    from promotions
    where ends_on > '2023-09-01'
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from valid a
    inner join promotions pr
        on a.channel <=> pr.channel
       and a.starts_on between pr.starts_on and pr.ends_on
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from with_window a
    inner join promotions pr
        on a.channel <=> pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_prefix,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        promotion_discount_pct_2,
        ntile(5) over (partition by promotion_promo_code order by promo_id asc, id asc, promo_id asc, promo_id_2 asc) as promo_id_bucket,
        row_number() over (partition by promotion_promo_code_2 order by promo_id_2 desc, id asc, promo_id asc, promo_id_2 asc) as event_seq,
        rank() over (order by promo_id_2 asc nulls first) as promo_id_2_rank,
        lag(discount_pct, 2, 0) over (partition by channel order by ends_on asc, id asc, promo_id asc, promo_id_2 asc) as prev_discount_pct
    from matched
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        promo_id,
        promo_id_2,
        promotion_promo_code,
        discount_pct,
        promotion_discount_pct_2,
        prev_discount_pct
    from metrics
) src
unpivot (
    metric_value for metric in (discount_pct, promotion_discount_pct_2, prev_discount_pct)
)
