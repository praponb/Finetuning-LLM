with attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions e
    left join promotions pr
        on coalesce(e.channel, 'store') = pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on < pr.ends_on + interval '7' day
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by promotion_promo_code, channel order by promo_id desc) as dedup_rank
        from attributed
    ) t
    where dedup_rank = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        lead(promotion_discount_pct) over w_grp as next_promotion_discount_pct,
        lead(discount_pct) over w_grp as next_discount_pct,
        lag(discount_pct) over w_grp as prev_discount_pct,
        row_number() over w_grp as seq_no,
        last_value(channel, true) over w_grp_2 as last_known_channel
    from deduped
    where promo_id != 6
    window w_grp as (partition by promo_code order by ends_on, id, promo_id), w_grp_2 as (partition by promo_code, promotion_promo_code order by ends_on, id, promo_id rows between unbounded preceding and current row)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        lag(seq_no) over (partition by promotion_promo_code order by next_discount_pct, id, promo_id) as prev_seq_no,
        lag(prev_discount_pct) over (partition by promotion_promo_code order by next_discount_pct, id, promo_id) as prev_prev_discount_pct,
        row_number() over (partition by promotion_promo_code order by next_discount_pct, id, promo_id) as event_seq,
        round(prev_discount_pct / sum(prev_discount_pct) over (partition by channel, promotion_promo_code), 4) as prev_discount_pct_share
    from scored
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by channel, promo_code order by prev_discount_pct desc nulls last, id, promo_id) as row_num
        from with_windows
    ) x
    where row_num = 1
)
select
    id,
    discount_pct,
    promo_id,
    promotion_discount_pct,
    dedup_rank,
    next_promotion_discount_pct,
    next_discount_pct,
    prev_discount_pct,
    last_known_channel
from ranked
order by next_promotion_discount_pct
