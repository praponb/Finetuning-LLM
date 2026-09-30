with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by city, region order by sqft) as dedup_rank
    from stores
    qualify dedup_rank = 1
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        dedup_rank,
        sum(sqft) over (order by sqft, id rows between unbounded preceding and current row) as cumulative_sqft,
        round(sqft / sum(sqft) over (partition by region), 4) as sqft_share,
        first_value(sqft, true) over (partition by region order by dedup_rank, id rows between unbounded preceding and current row) as first_known_sqft
    from ranked
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        ntile(10) over (partition by region order by first_known_sqft, id) as first_known_sqft_bucket,
        ntile(4) over (order by sqft_share desc, id) as sqft_share_bucket,
        lead(cumulative_sqft, 2) over (partition by city order by dedup_rank, id) as next_cumulative_sqft
    from with_windows
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        city,
        opened_date,
        dedup_rank,
        sqft_share,
        first_known_sqft,
        first_known_sqft_bucket,
        sqft_share_bucket,
        next_cumulative_sqft,
        concat(city, ', ', store_name) as city_store_name,
        coalesce(region, 'OTHER') as region_clean
    from enriched
    where opened_date > to_date('2024-02-28')
      and cumulative_sqft <> 54529
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by city order by first_known_sqft_bucket desc, id) as rank_in_group
        from valid
    ) ranked
    where rank_in_group = 1
),
with_windows_stores as (
    -- Running and moving aggregates
    select
        *,
        lead(next_cumulative_sqft, 1) over w_running as next_next_cumulative_sqft,
        row_number() over (partition by city order by first_known_sqft, id) as row_seq,
        row_number() over (partition by city order by first_known_sqft_bucket desc, id) as row_seq_2
    from latest
    window w_running as (order by first_known_sqft_bucket, id)
)
-- Multi-level aggregation
select
    city,
    date_trunc('year', opened_date) as year_start,
    avg(sqft_share) as avg_sqft_share,
    size(collect_set(city)) as num_cities,
    grouping_id() as grouping_level
from with_windows_stores
group by cube (city, date_trunc('year', opened_date))
order by year_start nulls first;
