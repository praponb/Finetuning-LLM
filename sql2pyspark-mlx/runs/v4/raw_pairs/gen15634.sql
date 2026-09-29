with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    from carts
    lateral view explode(items) li as line
),
lines as (
    -- Unnest the array into one row per element
    select
        product_id,
        qty,
        price,
        promo,
        value
    from flattened
    lateral view outer explode(coupon_codes) x as value
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, round(price * 100, 2) as adjusted_price
    from lines
    where promo is not null
      and price >= 200
),
valid_carts as (
    -- Keep recent, valid rows
    select
        product_id,
        qty,
        price,
        promo,
        value,
        adjusted_price,
        price + qty as price_plus_qty
    from valid
    where value in ('BUNDLE', 'VIP', 'SPRING10')
),
valid_carts_2 as (
    -- Filter the raw rows and derive helper columns
    select
        qty,
        price,
        value,
        adjusted_price,
        price_plus_qty,
        product_id + price as product_id_plus_price
    from valid_carts
    where price_plus_qty <= 36
),
metrics as (
    -- Running and moving aggregates
    select
        qty,
        price,
        value,
        adjusted_price,
        price_plus_qty,
        product_id_plus_price,
        max(price) over (partition by value) as value_max_price,
        max(product_id_plus_price) over (partition by value) as value_max_product_id_plus_price,
        cume_dist() over (order by qty nulls last) as qty_cume_dist
    from valid_carts_2
),
valid_carts_3 as (
    -- Basic cleansing and defaults
    select
        qty,
        price,
        value,
        adjusted_price,
        price_plus_qty,
        product_id_plus_price,
        value_max_price,
        value_max_product_id_plus_price,
        qty_cume_dist,
        ceil(value_max_price) as value_max_price_whole,
        coalesce(value, 'N/A') as value_clean
    from metrics
    where value is not null
      and product_id_plus_price < 22
),
daily as (
    -- Grouped metrics
    select
        value,
        size(collect_set(value)) as num_values,
        count_if(value = 'WELCOME5') as welcome5_events,
        percentile_approx(value_max_price, array(0.25, 0.5, 0.75)) as value_max_price_quartiles,
        percentile_approx(product_id_plus_price, array(0.25, 0.5, 0.75)) as product_id_plus_price_quartiles
    from valid_carts_3
    group by value
)
select
    value,
    value_max_price_quartiles,
    product_id_plus_price_quartiles,
    case when value rlike '^(VIP|SPRING10)$' then 'OTHER_ZONE' else 'PRIORITY' end as value_zone
from daily
order by value_zone desc nulls first, value;
