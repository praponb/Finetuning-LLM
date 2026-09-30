with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price as line_total
    from order_items
    where order_id = 5
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        product_id,
        unit_price
    from (
        select *, rank() over (partition by product_id order by unit_price desc nulls last) as row_num
        from valid
    ) ranked
    where row_num = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select *, round(unit_price * 1.15, 2) as unit_price_scaled
    from ranked
    where unit_price < 110
),
latest as (
    -- QUALIFY-style window filter
    select *
    from cleaned
    qualify row_number() over (partition by product_id order by unit_price_scaled desc, id) = 1
),
ranked_order_items as (
    -- Keep one row per key (latest first)
    select *
    from latest
    qualify row_number() over (partition by product_id order by unit_price_scaled nulls first, id) = 1
),
latest_order_items as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by product_id order by unit_price desc nulls last, id) as rn
    from ranked_order_items
    qualify rn = 1
),
enriched as (
    -- Attach products attributes
    select
        s.*,
        d.price,
        d.stock_qty
    from latest_order_items s
    left join products d
        on s.product_id = d.id
    where size(d.tags) > 1
)
select
    id,
    product_id,
    unit_price,
    unit_price_scaled,
    price
from enriched
order by price nulls first
