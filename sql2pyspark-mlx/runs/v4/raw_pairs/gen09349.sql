with with_windows as (
    select
        *,
        lead(stock_qty, 2) over (partition by supplier_id, brand order by price, id) next_stock_qty,
        avg(price) over (partition by category order by price, id rows between 3 preceding and current row) price_avg_3,
        first_value(price, true) over (order by stock_qty, id rows between unbounded preceding and current row) first_known_price
    from products
),
valid as (
    select
        id,
        product_name,
        category,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        next_stock_qty,
        length(category) category_length,
        lower(product_name) product_name_lower,
        coalesce(brand, 'UNKNOWN') resolved_brand
    from with_windows
    where launch_date <= '2024-04-01'
),
deduped as (
    select *, row_number() over (partition by category order by price desc nulls last, id) as rank_in_group
    from valid
    qualify rank_in_group = 1
),
cleaned as (
    select
        id,
        product_name,
        category,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        next_stock_qty,
        product_name_lower,
        resolved_brand,
        concat(category, '/', product_name) category_product_name
    from deduped
    where supplier_id is not null
)
select *
from (
    select
        supplier_id,
        category,
        next_stock_qty
    from cleaned
) src
pivot (
    avg(next_stock_qty) as total,
    count(1) as n
    for category in ('Grocery', 'Books')
)
