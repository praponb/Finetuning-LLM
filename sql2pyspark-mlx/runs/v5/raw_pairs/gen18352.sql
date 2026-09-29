with exploded as (
    -- Unnest the array into one row per element
    select
        t.id,
        t.created_at,
        t.coupon_codes,
        entry.product_id,
        entry.qty
    from carts t
    lateral view explode(t.items) li as entry
),
exploded_carts as (
    -- Unnest the array into one row per element
    select
        id,
        product_id,
        qty,
        explode_outer(coupon_codes) as code
    from exploded
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when code = 'WELCOME5' then 1 else 0 end as is_welcome5,
        lower(code) as code_lower,
        coalesce(code, 'DIRECT') as resolved_code
    from exploded_carts
    where code in ('SPRING10', 'VIP')
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (partition by code order by qty) as qty_rank,
        round(product_id / sum(product_id) over (partition by code), 4) as product_id_share,
        round(qty / sum(qty) over (partition by code), 4) as qty_share,
        stddev_samp(product_id) over (partition by code) as code_stddev_samp_product_id
    from valid
    where code = 'VIP'
),
scored as (
    -- Ranking and gap features
    select
        qty,
        is_welcome5,
        code_lower,
        qty_rank,
        product_id_share,
        qty_share,
        cume_dist() over (partition by code order by qty_rank desc nulls last) as qty_rank_cume_dist
    from metrics
),
with_windows as (
    -- Ranking and gap features
    select
        qty,
        is_welcome5,
        qty_rank,
        qty_share,
        qty_rank_cume_dist,
        rank() over (order by qty_rank_cume_dist desc nulls last) as qty_rank_cume_dist_rank,
        cume_dist() over (order by is_welcome5) as is_welcome5_cume_dist,
        rank() over (order by qty_rank) as qty_rank_rank,
        rank() over (order by qty_rank_cume_dist nulls last) as qty_rank_cume_dist_rank_2,
        cume_dist() over (order by product_id_share nulls last) as product_id_share_cume_dist
    from scored
),
valid_carts as (
    -- Filter the raw rows and derive helper columns
    select
        is_welcome5,
        qty_rank,
        qty_share,
        qty_rank_cume_dist,
        qty_rank_cume_dist_rank,
        is_welcome5_cume_dist,
        qty_rank_rank,
        qty_rank_cume_dist_rank_2,
        coalesce(qty_rank_cume_dist_rank, 0) as qty_rank_cume_dist_rank_filled
    from with_windows
    where qty_rank_cume_dist_rank <= 6
)
-- Multi-level aggregation
select
    case when is_welcome5_cume_dist >= 0.68 then 'HIGH' else 'LOW' end as is_welcome5_cume_dist_tier,
    case when qty_share >= 0.77 then 'PREMIUM' else 'STANDARD' end as qty_share_tier,
    case when qty_rank_cume_dist >= 0.87 then 'HIGH' else 'LOW' end as qty_rank_cume_dist_tier,
    round(avg(is_welcome5), 2) as avg_is_welcome5,
    approx_percentile(qty_rank_cume_dist_rank, 0.5) as qty_rank_cume_dist_rank_p50,
    count(*) as cnt
from valid_carts
group by case when is_welcome5_cume_dist >= 0.68 then 'HIGH' else 'LOW' end, case when qty_share >= 0.77 then 'PREMIUM' else 'STANDARD' end, case when qty_rank_cume_dist >= 0.87 then 'HIGH' else 'LOW' end
