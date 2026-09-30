with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    from carts
    lateral view outer explode(items) li as item
),
exploded_carts as (
    -- Unnest the array into one row per element
    select
        customer_id,
        created_at,
        product_id,
        price,
        value
    from exploded
    lateral view outer explode(coupon_codes) t as value
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        customer_id,
        created_at,
        product_id,
        price,
        value,
        product_id + price as product_id_plus_price,
        coalesce(product_id, 0) as product_id_filled,
        coalesce(value, 'DIRECT') as resolved_value
    from exploded_carts
    where created_at > date '2025-03-01'
      and customer_id is not null
      and value in ('WELCOME5', 'SPRING10', 'VIP')
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        max(product_id_plus_price) over (partition by customer_id order by cast(created_at as bigint) range between 1800 preceding and current row) as product_id_plus_price_max_30m,
        count(price) over (order by cast(created_at as bigint) range between 900 preceding and current row) as price_count_15m,
        rank() over (partition by customer_id order by product_id_plus_price desc nulls last) as product_id_plus_price_rank
    from cleaned
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (order by product_id_plus_price) as product_id_plus_price_dense_rank,
        avg(product_id) over (partition by value order by cast(created_at as bigint) range between 3600 preceding and current row) as product_id_avg_1h,
        percent_rank() over (order by product_id_plus_price desc) as product_id_plus_price_percent_rank
    from metrics
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        product_id_plus_price - product_id_filled as product_id_plus_price_minus_product_id_filled,
        coalesce(value, 'UNKNOWN') as value_filled
    from enriched
    where created_at > to_timestamp('2025-03-01 22:00:00')
      and customer_id is not null
),
kept_keys as (
    -- Set intersection on keys
    select customer_id
    from base
    intersect all
    select customer_id
    from carts
    where hour(created_at) between 6 and 10
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys k on s.customer_id = k.customer_id
)
-- Multi-level aggregation
select
    case when product_id_plus_price_rank >= 9 then 'LARGE' else 'SMALL' end as product_id_plus_price_rank_tier,
    value,
    percentile_approx(product_id_plus_price_rank, 0.9) as product_id_plus_price_rank_p90,
    sort_array(collect_set(value)) as value_list,
    sum(case when value = 'WELCOME5' then product_id_plus_price_minus_product_id_filled else 0 end) as welcome5_product_id_plus_price_minus_product_id_filled,
    sum(product_id_filled) filter (where value = 'VIP') as vip_total_product_id_filled,
    count_if(value = 'WELCOME5') as welcome5_events,
    min(created_at) as started_at
from kept_rows
group by case when product_id_plus_price_rank >= 9 then 'LARGE' else 'SMALL' end, value
having welcome5_events < 1
order by welcome5_events nulls last
