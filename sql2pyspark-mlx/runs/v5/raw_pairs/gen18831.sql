with with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions e
    left join promotions pr
        on coalesce(e.channel, 'store') = pr.channel
       and e.starts_on between pr.starts_on and pr.ends_on
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promotion_discount_pct - discount_pct promotion_discount_pct_minus_discount_pct,
        coalesce(channel, 'DIRECT') channel_filled
    from with_window
    where channel in ('mobile', 'store')
      and starts_on > to_date('2023-04-04')
),
with_window_promotions as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct
    from cleaned a
    left join promotions pr
        on coalesce(a.channel, 'web') = pr.channel
       and a.starts_on >= pr.starts_on
       and a.starts_on < pr.ends_on + interval '14' day
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by channel order by promo_id_2 desc, id, promo_id, promo_id_2) as rn
        from with_window_promotions
    ) ranked
    where rn = 1
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from ranked
    intersect
    select id
    from promotions
    where discount_pct <= 0.2
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    join remaining_keys k on s.id = k.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        round(promotion_discount_pct / sum(promotion_discount_pct) over (partition by promotion_promo_code), 4) promotion_discount_pct_share,
        round(promo_id_2 / sum(promo_id_2) over (partition by promotion_promo_code_2, channel), 4) promo_id_2_share
    from kept_rows
)
-- Multi-level aggregation
select
    date_trunc('quarter', ends_on) as quarter_start,
    round(sum(discount_pct) / nullif(count(case when promotion_promo_code_2 = 'VIP' then 1 end), 0), 2) as discount_pct_per_vip,
    max(promotion_discount_pct_share) as max_promotion_discount_pct_share
from with_windows
group by rollup (date_trunc('quarter', ends_on))
order by quarter_start nulls first;
