with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        lower(channel) as channel_lower,
        concat(channel, '/', promo_code) as channel_promo_code
    from promotions
    where starts_on < '2025-10-01'
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct as promotion_discount_pct
    from cleaned s
    left join promotions pr
        on s.channel = pr.channel
       and s.starts_on >= pr.starts_on
       and s.starts_on < pr.ends_on + interval 7 days
),
matched_promotions as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct_2
    from matched src
    left join promotions pr
        on coalesce(src.channel, 'store') = pr.channel
       and src.ends_on between pr.starts_on and pr.ends_on
),
remaining_keys as (
    -- Set intersection on keys
    select id, channel
    from matched_promotions
    intersect all
    select id, channel
    from promotions
    where discount_pct between 0.16 and 0.25
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from matched_promotions s
    join remaining_keys k on s.id = k.id and s.channel = k.channel
),
attributed as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id_3,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_3
    from kept_rows src
    left join promotions pr
        on src.channel = pr.channel
       and src.ends_on between pr.starts_on and pr.ends_on
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        channel,
        promotion_discount_pct,
        promotion_discount_pct_3
    from attributed
) src
unpivot (
    metric_value for measure in (promotion_discount_pct, promotion_discount_pct_3)
);
