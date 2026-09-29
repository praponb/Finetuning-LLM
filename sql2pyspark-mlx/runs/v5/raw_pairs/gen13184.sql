with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        month(opened_date) as opened_date_month,
        coalesce(region, 'OTHER') as region_clean
    from stores
    where region is not null
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from cleaned s
    where exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.quantity between 130 and 236
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and month(ch.last_restock) in (3, 4, 9)
        )
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        opened_date_month,
        max(opened_date_month) over (partition by region, city) as region_max_opened_date_month,
        ntile(5) over (partition by region, city order by opened_date_month, id) as opened_date_month_bucket,
        rank() over (partition by city, region order by opened_date_month desc nulls last) as opened_date_month_rank,
        ntile(10) over (partition by city order by opened_date_month desc, id) as opened_date_month_bucket_2,
        percent_rank() over (partition by city order by opened_date_month desc) as opened_date_month_percent_rank
    from not_flagged
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        region_max_opened_date_month,
        opened_date_month_bucket,
        opened_date_month_rank,
        opened_date_month_bucket_2,
        opened_date_month_percent_rank,
        cast(opened_date_month_bucket as double) as opened_date_month_bucket_num,
        coalesce(region, 'DIRECT') as region_filled
    from metrics
    where opened_date > date '2023-07-14'
      and region = 'West'
)
-- Multi-level aggregation
select
    date_trunc('year', opened_date) as year_start,
    case when opened_date_month_percent_rank >= 0.62 then 'HIGH' else 'LOW' end as opened_date_month_percent_rank_tier,
    sum(opened_date_month_percent_rank) as total_opened_date_month_percent_rank,
    count(*) as event_count,
    count(distinct region) as distinct_regions,
    count(distinct region) as distinct_regions_2,
    count_if(city = 'New York') as new_york_events,
    count(case when city = 'Singapore' then 1 end) as singapore_count
from valid
group by year_start, opened_date_month_percent_rank_tier;
