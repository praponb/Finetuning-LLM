with attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions a
    left join promotions pr
        on coalesce(a.channel, 'web') = pr.channel
       and a.starts_on >= pr.starts_on
       and a.starts_on <= date_add(pr.ends_on, 10)
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by promotion_promo_code, channel order by discount_pct desc, id asc, promo_id asc) as dedup_rank
        from attributed
    ) x
    where dedup_rank = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        dedup_rank,
        lag(promotion_discount_pct, 2) over (partition by channel order by promo_id asc, id asc, promo_id asc) as prev_promotion_discount_pct,
        cume_dist() over (order by promotion_discount_pct desc nulls last) as promotion_discount_pct_cume_dist,
        sum(promotion_discount_pct) over (partition by promo_code, channel order by starts_on asc, id asc, promo_id asc rows between unbounded preceding and current row) as running_promotion_discount_pct
    from latest
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by promotion_promo_code, channel order by prev_promotion_discount_pct desc, id asc, promo_id asc) as row_seq,
        first_value(prev_promotion_discount_pct) ignore nulls over (order by promotion_discount_pct_cume_dist asc, id asc, promo_id asc rows between unbounded preceding and current row) as first_known_prev_promotion_discount_pct,
        row_number() over (partition by promotion_promo_code order by promo_id desc, id asc, promo_id asc) as event_seq,
        stddev_samp(running_promotion_discount_pct) over (partition by promotion_promo_code) as promotion_promo_code_stddev_samp_running_promotion_discount_pct,
        min(running_promotion_discount_pct) over (partition by promo_code) as promo_code_min_running_promotion_discount_pct
    from with_windows
),
latest_promotions as (
    -- Keep one row per key (latest first)
    select *
    from metrics
    qualify row_number() over (partition by channel order by discount_pct desc, id asc, promo_id asc) = 1
),
with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from latest_promotions s
    join promotions pr
        on s.channel = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on < pr.ends_on + interval 30 days
)
select
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    promo_id,
    promotion_discount_pct,
    dedup_rank,
    prev_promotion_discount_pct,
    case when channel rlike '^(mobile|store)$' then 'GROUP_B' when channel rlike '^(web|partner)$' then 'OTHER_ZONE' else 'PRIORITY' end as channel_zone
from with_window;
