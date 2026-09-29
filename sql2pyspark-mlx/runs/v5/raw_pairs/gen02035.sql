with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price
    from (
        select
            id,
            created_at,
            coupon_codes,
            explode_outer(items) as item
        from carts
    ) t
)
-- Aggregate with conditional metrics
select
    case when qty >= 5 then 'PREMIUM' else 'STANDARD' end as qty_tier,
    min(created_at) as first_created_at,
    approx_percentile(qty, array(0.25, 0.5, 0.75)) as qty_quartiles,
    count(*) as cnt,
    sum(product_id) as total_product_id,
    approx_percentile(product_id, 0.5) as product_id_p50,
    max(created_at) as last_seen
from exploded
group by 1;
