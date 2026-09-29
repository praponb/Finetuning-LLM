with with_dims as (
    -- Attach suppliers attributes
    select
        s.*,
        p.city,
        p.rating,
        p.supplier_name
    from products s
    join suppliers p
        on s.supplier_id = p.id
),
joined as (
    -- Attach suppliers attributes
    select e.*, p.rating as supplier_rating
    from with_dims e
    join suppliers p
        on e.supplier_id = p.id
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, latest.store_sale_units
    from joined s
    join lateral (
            select c.units as store_sale_units
            from store_sales c
            where c.product_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) latest on true
),
enriched as (
    -- Running and moving aggregates
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
        city,
        rating,
        supplier_name,
        supplier_rating,
        store_sale_units,
        row_number() over (partition by city order by cost asc, id asc) as seq_no,
        sum(stock_qty) over (order by price asc, id asc rows between unbounded preceding and current row) as cumulative_stock_qty,
        stddev_samp(rating) over (partition by brand) as brand_stddev_samp_rating,
        last_value(brand) ignore nulls over (order by rating asc, id asc rows between unbounded preceding and current row) as last_known_brand,
        round(supplier_rating / sum(supplier_rating) over (partition by supplier_id), 4) as supplier_rating_share
    from with_last
)
select
    id,
    category,
    price,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    city,
    rating,
    case when category rlike '^(Books|Home)$' then 'EXTENDED' when category rlike '^(Electronics|Sports)$' then 'CORE' else 'OTHER_ZONE' end as category_zone
from enriched
order by id desc nulls last, category_zone asc
limit 10;
