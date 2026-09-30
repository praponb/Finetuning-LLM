with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        line.product_id,
        line.qty,
        line.promo
    from carts
    lateral view posexplode(items) li as pos, line
),
lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        pos,
        qty,
        promo,
        explode_outer(coupon_codes) as value
    from flattened
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        created_at,
        pos,
        qty,
        promo,
        value,
        max(pos) over (partition by value, customer_id) value_max_pos,
        rank() over (partition by customer_id order by pos desc nulls first) pos_rank,
        max(qty) over (partition by value) value_max_qty,
        cume_dist() over (partition by promo, value order by pos) pos_cume_dist,
        stddev_samp(pos) over (partition by promo) promo_stddev_samp_pos
    from lines
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        created_at,
        pos,
        qty,
        promo,
        value,
        value_max_pos,
        pos_rank,
        value_max_qty,
        value_max_pos + pos value_max_pos_plus_pos,
        coalesce(value, 'N/A') value_filled
    from with_windows
    where created_at >= timestamp '2025-03-02 14:00:00'
      and value is not null
      and promo = 'WELCOME5'
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        created_at,
        pos,
        qty,
        promo,
        value,
        value_max_pos,
        pos_rank,
        value_max_qty,
        value_max_pos_plus_pos,
        value_filled,
        dense_rank() over (partition by value order by value_max_qty desc nulls first) value_max_qty_dense_rank,
        max(qty) over (partition by promo) promo_max_qty
    from valid
)
-- Multi-level aggregation
select
    date_trunc('day', created_at) as activity_day,
    case when pos >= 3 then 'LARGE' else 'SMALL' end as pos_tier,
    count(*) as event_count,
    sum(case when value = 'BUNDLE' then pos_rank else 0 end) as bundle_pos_rank,
    count(distinct id) as distinct_ids,
    sum(promo_max_qty) filter (where promo = 'VIP') as vip_total_promo_max_qty
from metrics
group by cube (date_trunc('day', created_at), case when pos >= 3 then 'LARGE' else 'SMALL' end)
having bundle_pos_rank >= 0
order by event_count desc
