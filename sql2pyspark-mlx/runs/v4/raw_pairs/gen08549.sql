with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        round(discount_pct * 0.85, 2) as discount_pct_adjusted,
        last_day(ends_on) as ends_on_month_end
    from promotions
    where ends_on > date '2024-06-02'
      and channel is not null
      and channel in ('mobile', 'partner', 'store')
),
enriched as (
    -- Ranking and gap features
    select
        *,
        last_value(discount_pct_adjusted) ignore nulls over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as last_known_discount_pct_adjusted,
        dense_rank() over (order by discount_pct_adjusted desc nulls last) as discount_pct_adjusted_dense_rank,
        lag(discount_pct_adjusted, 2) over (partition by channel order by starts_on, id) as prev_discount_pct_adjusted
    from base
),
with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct as promotion_discount_pct
    from enriched e
    join promotions pr
        on e.channel = pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on < pr.ends_on + interval 7 days
),
with_window_promotions as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct_2
    from with_window a
    left join promotions pr
        on a.channel <=> pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
)
-- Window metrics over several different partitions
select
    id,
    channel,
    starts_on,
    ends_on,
    discount_pct,
    discount_pct_adjusted,
    ends_on_month_end,
    last_known_discount_pct_adjusted,
    discount_pct_adjusted_dense_rank,
    prev_discount_pct_adjusted,
    promo_id,
    promo_code,
    promotion_discount_pct,
    promo_id_2,
    promotion_promo_code,
    promotion_discount_pct_2,
    row_number() over (partition by channel order by discount_pct_adjusted desc, id, promo_id, promo_id_2) as seq_no,
    avg(discount_pct) over (partition by channel) as channel_avg_discount_pct
from with_window_promotions;
