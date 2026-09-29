with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    from (
        select
            id,
            coupon_codes,
            explode(items) as elem
        from carts
    ) x
),
flattened as (
    -- Unnest the array into one row per element
    select
        qty,
        price,
        tag
    from exploded
    lateral view explode(coupon_codes) li as tag
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by tag order by qty desc nulls first) as qty_rank,
        avg(price) over (partition by tag) as tag_avg_price,
        round(price / sum(price) over (partition by tag), 4) as price_share
    from flattened
),
grouped as (
    -- Aggregate with conditional metrics
    select
        tag,
        count_if(tag = 'BUNDLE') as bundle_events,
        count(*) filter (where tag = 'SPRING10') as spring10_rows
    from enriched
    group by tag
)
select tag, spring10_rows
from grouped
order by tag desc nulls first;
