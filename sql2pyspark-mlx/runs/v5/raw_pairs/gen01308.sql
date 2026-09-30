with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), launch_date) as days_since_launch_date,
        concat(product_name, ' ', category) as product_name_category,
        coalesce(brand, 'DIRECT') as brand_clean
    from products
    where price between 380 and 710
),
cleaned_products as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        days_since_launch_date,
        product_name_category,
        brand_clean,
        round(price, 1) as price_rounded
    from cleaned
    where launch_date >= date '2023-05-30'
      and brand is not null
),
joined as (
    -- Attach suppliers attributes
    select
        src.*,
        s.supplier_name,
        s.country,
        s.rating
    from cleaned_products src
    left join suppliers s
        on src.supplier_id = s.id
    where s.country = 'US'
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.cost,
        s.launch_date,
        s.brand_clean,
        s.supplier_name,
        s.country,
        latest.order_item_quantity,
        latest.unit_price
    from joined s
    join lateral (
            select c.quantity as order_item_quantity, c.unit_price
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
)
-- Aggregate with conditional metrics
select
    category,
    country,
    max(order_item_quantity) as max_order_item_quantity,
    avg(cost) filter (where brand = 'Stark') as stark_avg_cost
from with_latest
group by category, country
order by country nulls last
