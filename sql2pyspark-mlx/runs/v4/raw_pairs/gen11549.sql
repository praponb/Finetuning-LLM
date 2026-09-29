with base as (
    -- Keep recent, valid rows
    select *, initcap(trim(promo_code)) as promo_code_title
    from promotions
    where ends_on > to_date('2023-10-03')
      and channel is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        min(discount_pct) over (partition by promo_code) as promo_code_min_discount_pct,
        round(discount_pct / sum(discount_pct) over (partition by channel), 4) as discount_pct_share,
        lead(discount_pct, 1) over (partition by promo_code order by discount_pct, id) as next_discount_pct
    from base
),
scored_promotions as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_title,
        promo_code_min_discount_pct,
        discount_pct_share,
        next_discount_pct,
        cume_dist() over (partition by promo_code order by promo_code_min_discount_pct nulls first) as promo_code_min_discount_pct_cume_dist,
        lead(discount_pct_share, 2) over (partition by promo_code, channel order by discount_pct, id) as next_discount_pct_share,
        ntile(4) over (partition by promo_code order by discount_pct_share desc, id) as discount_pct_share_bucket,
        cume_dist() over (partition by channel, promo_code order by discount_pct desc) as discount_pct_cume_dist,
        avg(promo_code_min_discount_pct) over (partition by channel order by discount_pct_share, id rows between 6 preceding and current row) as promo_code_min_discount_pct_avg_6
    from scored
)
select
    id,
    channel,
    starts_on,
    ends_on,
    discount_pct,
    promo_code_title,
    promo_code_min_discount_pct,
    next_discount_pct,
    promo_code_min_discount_pct_cume_dist,
    case when channel rlike '^(store|web)$' then 'GROUP_B' when channel rlike '^(mobile|partner)$' then 'GROUP_A' else 'EXTENDED' end as channel_zone
from scored_promotions
order by promo_code_min_discount_pct_cume_dist nulls last, id;
