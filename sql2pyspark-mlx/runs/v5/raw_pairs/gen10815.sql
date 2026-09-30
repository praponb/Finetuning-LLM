with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        inline(items)
    from carts
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(promo, 'N/A') as promo_filled
    from exploded
    where customer_id is null
),
cleaned_carts as (
    -- Filter the raw rows and derive helper columns
    select *, array_join(coupon_codes, '|') as coupon_codes_text
    from cleaned
    where promo is not null
),
metrics as (
    -- Window metrics over several different partitions
    select *, cume_dist() over (partition by promo order by product_id nulls last) as product_id_cume_dist
    from cleaned_carts
    where promo not in ('VIP', 'SPRING10')
),
enriched as (
    -- Ranking and gap features
    select *, round(qty / sum(qty) over (partition by promo, customer_id), 4) as qty_share
    from metrics
)
-- Aggregate with conditional metrics
select
    case when qty_share >= 0.64 then 'PREMIUM' else 'STANDARD' end as qty_share_tier,
    product_id,
    percentile_approx(product_id, 0.9) as product_id_p90,
    round(sum(product_id) / nullif(count(case when promo = 'BUNDLE' then 1 end), 0), 2) as product_id_per_bundle,
    rank() over (partition by case when qty_share >= 0.64 then 'PREMIUM' else 'STANDARD' end order by round(sum(product_id) / nullif(count(case when promo = 'BUNDLE' then 1 end), 0), 2) desc) as product_id_per_bundle_rank
from enriched
group by case when qty_share >= 0.64 then 'PREMIUM' else 'STANDARD' end, product_id
order by product_id_per_bundle desc nulls last;
