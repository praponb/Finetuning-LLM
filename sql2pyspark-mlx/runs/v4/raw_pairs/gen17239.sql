with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(trim(store_name)) as store_name_normalized,
        coalesce(region, 'N/A') as region_clean
    from stores
    where region is not null
      and sqft <= 5931
),
cleaned_stores as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(region)) as region_title,
        coalesce(region, 'UNKNOWN') as resolved_region
    from cleaned
    where opened_date >= date '2023-08-24'
      and region is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        store_name_normalized,
        region_clean,
        region_title,
        resolved_region,
        ntile(3) over (partition by city order by sqft, id) as sqft_bucket,
        sum(sqft) over (order by sqft, id rows between unbounded preceding and current row) as cumulative_sqft
    from cleaned_stores
)
-- Grouped metrics
select
    date_trunc('week', opened_date) as week_start,
    city,
    case when sqft >= 17312 then 'LARGE' else 'SMALL' end as sqft_tier,
    approx_percentile(cumulative_sqft, 0.5) as cumulative_sqft_p50,
    round(avg(sqft), 2) as avg_sqft,
    bit_or(sqft) as sqft_bit_or,
    bit_and(sqft) as sqft_bit_and,
    grouping(date_trunc('week', opened_date)) as grouping_id
from enriched
group by rollup (date_trunc('week', opened_date), city, case when sqft >= 17312 then 'LARGE' else 'SMALL' end)
having approx_percentile(cumulative_sqft, 0.5) < 0
order by week_start desc
