with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ceil(revenue) as revenue_whole,
        round(units, 0) as units_rounded
    from store_sales
    where product_id in (1, 2, 4, 7)
),
filtered_store_sales as (
    -- Filter the raw rows and derive helper columns
    select *, cast(revenue_whole as string) as revenue_whole_num
    from filtered
    where sale_date > to_date('2023-06-17')
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, revenue_whole + units_rounded as revenue_whole_plus_units_rounded
    from filtered_store_sales
    where store_id = 4
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        store_id,
        product_id,
        units,
        revenue,
        units_rounded,
        revenue_whole_plus_units_rounded
    from base
    qualify rank() over (partition by store_id order by revenue desc nulls last) = 1
),
base_store_sales as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        revenue,
        revenue_whole_plus_units_rounded,
        ceil(revenue) as revenue_whole
    from latest
    where units = 18
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        revenue_whole,
        revenue - revenue_whole_plus_units_rounded as revenue_minus_revenue_whole_plus_units_rounded,
        least(revenue_whole_plus_units_rounded, revenue) as least_revenue_whole_plus_units_rounded_revenue
    from base_store_sales
    where revenue < 3000
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        least_revenue_whole_plus_units_rounded_revenue,
        ntile(10) over (partition by product_id order by least_revenue_whole_plus_units_rounded_revenue desc, id) as least_revenue_whole_plus_units_rounded_revenue_bucket,
        rank() over (partition by product_id order by revenue_whole) as revenue_whole_rank,
        rank() over (partition by product_id order by least_revenue_whole_plus_units_rounded_revenue desc nulls last) as least_revenue_whole_plus_units_rounded_revenue_rank
    from valid
    where least_revenue_whole_plus_units_rounded_revenue <= 67
)
-- Grouped metrics
select
    case when least_revenue_whole_plus_units_rounded_revenue >= 69 then 'PREMIUM' else 'STANDARD' end as least_revenue_whole_plus_units_rounded_revenue_tier,
    count(distinct id) as distinct_ids,
    percentile_approx(least_revenue_whole_plus_units_rounded_revenue_bucket, array(0.25, 0.5, 0.75)) as least_revenue_whole_plus_units_rounded_revenue_bucket_quartiles
from enriched
group by case when least_revenue_whole_plus_units_rounded_revenue >= 69 then 'PREMIUM' else 'STANDARD' end
having count(distinct id) >= 1
order by least_revenue_whole_plus_units_rounded_revenue_tier nulls first
