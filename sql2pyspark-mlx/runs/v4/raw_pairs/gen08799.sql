with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        attr_key,
        value
    from carts
    lateral view outer explode(attributes) x as attr_key, value
),
cleaned as (
    -- Keep recent, valid rows
    select *, size(coupon_codes) as coupon_codes_count
    from flattened
    where attr_key is not null
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        value,
        coupon_codes_count,
        round(coupon_codes_count, 1) as coupon_codes_count_rounded
    from cleaned
    where attr_key is not null
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by coupon_codes_count desc nulls first) as coupon_codes_count_rank,
        cume_dist() over (order by coupon_codes_count_rounded desc) as coupon_codes_count_rounded_cume_dist,
        dense_rank() over (order by coupon_codes_count desc nulls last) as coupon_codes_count_dense_rank
    from valid
),
enriched_carts as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (order by coupon_codes_count_rank desc nulls last) as coupon_codes_count_rank_cume_dist,
        cume_dist() over (order by coupon_codes_count_dense_rank nulls first) as coupon_codes_count_dense_rank_cume_dist,
        rank() over (order by coupon_codes_count_rounded_cume_dist desc) as coupon_codes_count_rounded_cume_dist_rank
    from enriched
)
-- Aggregate with conditional metrics
select
    case when coupon_codes_count_rounded >= 86 then 'LARGE' else 'SMALL' end as coupon_codes_count_rounded_tier,
    min(coupon_codes_count_dense_rank_cume_dist) as min_coupon_codes_count_dense_rank_cume_dist,
    percentile_approx(coupon_codes_count_dense_rank, array(0.25, 0.5, 0.75)) as coupon_codes_count_dense_rank_quartiles,
    max(coupon_codes_count_rounded) as max_coupon_codes_count_rounded,
    count(*) as row_count
from enriched_carts
group by case when coupon_codes_count_rounded >= 86 then 'LARGE' else 'SMALL' end
order by min_coupon_codes_count_dense_rank_cume_dist desc
