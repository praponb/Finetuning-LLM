with valid as (
    -- Keep recent, valid rows
    select
        *,
        length(channel) as channel_length,
        shipping_fee - amount as amount_with_shipping,
        coalesce(channel, 'NONE') as channel_filled
    from orders
    where order_date > date '2023-08-07'
      and channel is not null
),
attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from valid a
    left join promotions pr
        on a.channel = pr.channel
       and a.order_date >= pr.starts_on
       and a.order_date < pr.ends_on + interval '7' day
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by discount desc nulls first) as discount_cume_dist,
        round(amount / sum(amount) over (partition by customer_id), 4) as amount_share
    from attributed
    where promo_code not in ('WELCOME5', 'BUNDLE', 'VIP')
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from scored e
    left join promotions pr
        on e.channel <=> pr.channel
       and e.order_date between pr.starts_on and pr.ends_on
)
-- Aggregate with conditional metrics
select
    date_trunc('week', order_date) as week_start,
    round(sum(promotion_discount_pct) / nullif(count(case when channel = 'store' then 1 end), 0), 2) as promotion_discount_pct_per_store,
    percentile_approx(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    count(case when status = 'COMPLETED' then 1 end) as completed_count
from matched
group by date_trunc('week', order_date)
