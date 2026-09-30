with cleaned as (
    -- Basic cleansing and defaults
    select *, upper(brand) as brand_upper
    from products
    where brand is not null
),
with_windows as (
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
        brand_upper,
        first_value(brand) ignore nulls over (partition by category order by cost, id rows between unbounded preceding and current row) as first_known_brand,
        last_value(stock_qty, true) over (order by stock_qty, id rows between unbounded preceding and current row) as last_known_stock_qty
    from cleaned
),
metrics as (
    -- Window metrics over several different partitions
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
        brand_upper,
        first_known_brand,
        last_known_stock_qty,
        ntile(3) over w_hist as last_known_stock_qty_bucket,
        first_value(cost, true) over (partition by category order by last_known_stock_qty, id rows between unbounded preceding and current row) as first_known_cost
    from with_windows
    window w_hist as (partition by category order by last_known_stock_qty desc, id)
),
with_dims as (
    -- Attach suppliers attributes
    select
        a.*,
        d.country,
        d.city
    from metrics a
    left join suppliers d
        on a.supplier_id = d.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        trim(country) as clean_country,
        coalesce(brand, 'NONE') as brand_clean
    from with_dims
    where launch_date >= date '2024-03-22'
      and city is not null
      and price < 1090
),
joined as (
    -- Attach suppliers attributes
    select
        e.*,
        p.rating,
        p.city as supplier_city,
        p.country as supplier_country
    from filtered e
    inner join suppliers p
        on e.supplier_id = p.id
)
-- Aggregate with conditional metrics
select
    case when last_known_stock_qty_bucket >= 8 then 'LARGE' else 'SMALL' end as last_known_stock_qty_bucket_tier,
    date_trunc('week', launch_date) as week_start,
    percentile_approx(rating, 0.5) as rating_p50,
    count(*) filter (where supplier_country = 'DE') as de_rows
from joined
group by cube (case when last_known_stock_qty_bucket >= 8 then 'LARGE' else 'SMALL' end, date_trunc('week', launch_date));
