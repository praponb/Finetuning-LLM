with joined as (
    -- Attach stores attributes
    select
        e.id,
        e.store_id,
        e.product_id,
        e.sale_date,
        b.sqft,
        b.store_name
    from store_sales e
    left join stores b
        on e.store_id = b.id
),
with_dims as (
    -- Attach stores attributes
    select
        src.*,
        b.opened_date,
        b.store_name as store_store_name,
        b.sqft as store_sqft
    from joined src
    left join stores b
        on src.store_id = b.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        sale_date,
        sqft,
        opened_date,
        store_store_name,
        ntile(3) over (partition by product_id order by store_sqft, id) as store_sqft_bucket,
        rank() over (partition by product_id, store_id order by store_sqft desc) as store_sqft_rank
    from with_dims
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        sale_date,
        opened_date,
        store_sqft_bucket,
        store_sqft_rank,
        sqft - store_sqft_bucket as sqft_minus_store_sqft_bucket
    from with_windows
    where sale_date >= date '2023-05-05'
)
-- Grouped metrics
select
    case when store_sqft_bucket >= 8 then 'PREMIUM' else 'STANDARD' end as store_sqft_bucket_tier,
    round(avg(sqft_minus_store_sqft_bucket), 2) as avg_sqft_minus_store_sqft_bucket,
    percentile_approx(store_sqft_bucket, 0.95) as store_sqft_bucket_p95,
    approx_percentile(store_sqft_bucket, 0.9) as store_sqft_bucket_p90,
    avg(store_sqft_rank) as avg_store_sqft_rank,
    round(avg(sqft_minus_store_sqft_bucket), 2) as avg_sqft_minus_store_sqft_bucket_2,
    percentile_approx(sqft_minus_store_sqft_bucket, array(0.25, 0.5, 0.75)) as sqft_minus_store_sqft_bucket_quartiles
from cleaned
group by case when store_sqft_bucket >= 8 then 'PREMIUM' else 'STANDARD' end
having percentile_approx(store_sqft_bucket, 0.95) < 1000000;
