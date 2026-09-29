with valid as (
    -- Keep recent, valid rows
    select
        *,
        round(sqft, 0) as sqft_rounded,
        coalesce(region, 'DIRECT') as region_clean
    from stores
    where opened_date > date '2023-06-18'
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by region, city order by sqft asc, id asc) as dedup_rank
        from valid
    ) t
    where dedup_rank = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(sqft_rounded) over (partition by city order by sqft asc, id asc rows between unbounded preceding and current row) as running_sqft_rounded,
        sum(sqft_rounded) over (partition by city, region order by opened_date asc, id asc rows between unbounded preceding and current row) as cumulative_sqft_rounded,
        sum(sqft_rounded) over (order by sqft asc, id asc rows between unbounded preceding and current row) as cumulative_sqft_rounded_2,
        lag(sqft, 2, 0) over (partition by region, city order by opened_date asc, id asc) as prev_sqft,
        ntile(4) over (partition by region order by sqft desc, id asc) as sqft_bucket
    from ranked
),
valid_stores as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_rounded,
        region_clean,
        dedup_rank,
        cumulative_sqft_rounded_2,
        prev_sqft,
        sqft_bucket,
        upper(store_name) as store_name_upper,
        sqft * sqft_rounded as sqft_times_sqft_rounded,
        coalesce(region, 'N/A') as resolved_region
    from enriched
    where region is not null
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by region, city order by opened_date asc, id asc) as rank_in_group
        from valid_stores
    ) x
    where rank_in_group = 1
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        region,
        sqft_times_sqft_rounded,
        sqft_rounded
    from latest
) src
unpivot include nulls (
    reading for metric_name in (sqft_times_sqft_rounded as SQFT_TIMES_SQFT_ROUNDED_VAL, sqft_rounded as SQFT_ROUNDED)
)
