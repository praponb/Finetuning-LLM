with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(category) clean_category,
        coalesce(brand, 'NONE') brand_clean
    from products
    where stock_qty <= 146
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        dayofweek(launch_date) launch_date_dayofweek,
        replace(product_name, ' ', '_') product_name_slug,
        coalesce(brand, 'N/A') resolved_brand
    from valid
    where brand is not null
),
scored as (
    -- Ranking and gap features
    select
        *,
        min(launch_date_dayofweek) over (partition by supplier_id, brand order by price, id rows between unbounded preceding and current row) running_min_launch_date_dayofweek,
        sum(launch_date_dayofweek) over (partition by supplier_id, brand order by price, id rows between unbounded preceding and current row) running_sum_launch_date_dayofweek,
        count(launch_date_dayofweek) over (partition by supplier_id, brand order by price, id rows between unbounded preceding and current row) running_count_launch_date_dayofweek
    from filtered
    where category = 'Fashion'
),
ranked as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by category, brand order by running_sum_launch_date_dayofweek nulls last) as row_num
    from scored
    qualify row_num = 1
)
-- Join a dimension and aggregate
select
    d.country,
    a.brand,
    count(*) as num_rows,
    sum(a.running_count_launch_date_dayofweek) as total_running_count_launch_date_dayofweek,
    grouping_id() as gid
from ranked a
inner join suppliers d on a.supplier_id = d.id
group by grouping sets ((d.country, a.brand), (d.country), ())
order by brand nulls first
