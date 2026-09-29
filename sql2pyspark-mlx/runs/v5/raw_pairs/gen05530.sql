with cleaned as (
    -- Keep recent, valid rows
    select *, round(discount_pct * 100, 2) as discount_pct_scaled
    from promotions
    where channel = 'partner'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lag(discount_pct) over (partition by channel, promo_code order by discount_pct_scaled, id) as prev_discount_pct,
        lead(discount_pct_scaled) over (partition by channel, promo_code order by discount_pct_scaled, id) as next_discount_pct_scaled,
        lag(discount_pct_scaled) over (partition by channel, promo_code order by discount_pct_scaled, id) as prev_discount_pct_scaled,
        row_number() over (partition by channel, promo_code order by discount_pct_scaled, id) as event_seq,
        sum(discount_pct_scaled) over (partition by promo_code, channel order by ends_on, id rows between unbounded preceding and current row) as cumulative_discount_pct_scaled
    from cleaned
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from with_windows a
    inner join promotions pr
        on coalesce(a.channel, 'web') = pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
),
with_windows_promotions as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        prev_discount_pct_scaled,
        event_seq,
        promo_id,
        promotion_promo_code,
        avg(prev_discount_pct) over (partition by promo_code order by promotion_discount_pct, id, promo_id rows between 3 preceding and current row) as prev_discount_pct_avg_3,
        row_number() over (partition by promotion_promo_code order by promotion_discount_pct desc, id, promo_id) as event_seq_2
    from matched
)
-- Native SQL PIVOT
select *
from (
    select
        promotion_promo_code,
        channel,
        promo_code,
        prev_discount_pct_scaled
    from with_windows_promotions
) src
pivot (
    max(prev_discount_pct_scaled)
    for promo_code in ('BUNDLE', 'WELCOME5', 'SPRING10')
)
