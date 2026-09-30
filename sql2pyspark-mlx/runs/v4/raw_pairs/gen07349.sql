with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        item.product_id,
        item.promo
    from carts
    lateral view explode(items) x as item
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        array_join(coupon_codes, ', ') as coupon_codes_text,
        coalesce(promo, 'UNKNOWN') as promo_clean
    from exploded
    where promo is not null
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by promo order by product_id) as product_id_percent_rank,
        min(product_id) over (partition by promo) as promo_min_product_id,
        count(product_id) over (partition by promo) as promo_count_product_id
    from cleaned
)
-- Multi-level aggregation
select
    case when product_id >= 8 then 'PREMIUM' else 'STANDARD' end as product_id_tier,
    count(case when promo = 'VIP' then 1 end) as vip_count,
    approx_percentile(product_id_percent_rank, 0.5) as product_id_percent_rank_p50,
    count(*) filter (where promo = 'SPRING10') as spring10_rows
from enriched
group by case when product_id >= 8 then 'PREMIUM' else 'STANDARD' end
