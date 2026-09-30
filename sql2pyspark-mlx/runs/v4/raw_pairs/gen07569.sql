with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        concat(brand, ' ', product_name) as brand_product_name,
        date_sub(launch_date, 61) as launch_date_minus_61d
    from products
    where launch_date >= date '2023-05-29'
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        brand_product_name,
        launch_date_minus_61d,
        array_contains(tags, 'sale') as is_sale,
        price * stock_qty as price_times_stock_qty,
        coalesce(brand, 'OTHER') as resolved_brand
    from cleaned
    where launch_date >= date '2023-04-10'
      and brand is not null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from valid
    qualify row_number() over (partition by category, brand order by price_times_stock_qty nulls first, id) = 1
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.inventory_quantity,
        x.last_restock
    from deduped s
    join lateral (
            select c.quantity as inventory_quantity, c.last_restock
            from inventory c
            where c.product_id = s.id
              and c.last_restock <= s.launch_date
            order by c.last_restock desc, c.id desc
            limit 1
        ) x on true
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.product_id in (2, 4, 6)
        )
),
with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.inventory_last_restock,
        x.quantity
    from not_flagged s
    join lateral (
            select c.last_restock as inventory_last_restock, c.quantity
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
)
-- Grouped metrics
select
    date_trunc('month', launch_date) as activity_month,
    approx_percentile(cost, 0.9) as cost_p90,
    count_if(category = 'Grocery') as grocery_events,
    bit_or(stock_qty) as stock_qty_bit_or,
    sum(case when brand = 'Initech' then stock_qty else 0 end) as initech_stock_qty,
    avg(stock_qty) as avg_stock_qty,
    size(collect_set(category)) as num_categories
from with_last
group by activity_month
having stock_qty_bit_or < 1;
