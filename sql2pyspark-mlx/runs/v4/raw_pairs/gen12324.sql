with valid as (
    select
        *,
        ceil(discount_pct) as discount_pct_whole,
        initcap(trim(channel)) as channel_title,
        coalesce(channel, 'UNKNOWN') as channel_filled
    from promotions
    where channel is not null
),
metrics as (
    select
        *,
        dense_rank() over (partition by promo_code order by discount_pct_whole nulls first) as discount_pct_whole_dense_rank,
        lead(discount_pct_whole, 1, 0) over w_recent as next_discount_pct_whole
    from valid
    window w_recent as (partition by channel order by discount_pct, id)
),
with_window as (
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from metrics s
    left join promotions pr
        on s.channel = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on < pr.ends_on + interval '7' day
),
with_window_promotions as (
    select
        e.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from with_window e
    join promotions pr
        on coalesce(e.channel, 'web') = pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on < pr.ends_on + interval '7' day
)
select
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct,
    discount_pct_whole,
    channel_title,
    channel_filled,
    discount_pct_whole_dense_rank,
    case when channel rlike '^(web|partner)$' then 'GROUP_A' when channel rlike '^(mobile|store)$' then 'EXTENDED' else 'PRIORITY' end as channel_zone
from with_window_promotions
order by discount_pct_whole_dense_rank desc nulls first
