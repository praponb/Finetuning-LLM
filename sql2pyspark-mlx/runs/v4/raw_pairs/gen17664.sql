with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        case when country = 'US' then 1 else 0 end as is_us,
        case when city = 'Bangkok' then 1 else 0 end as is_bangkok
    from suppliers
    where city is not null
),
enriched as (
    -- Ranking and gap features
    select
        *,
        round(rating / sum(rating) over (partition by country), 4) as rating_share,
        cume_dist() over (order by rating nulls last) as rating_cume_dist
    from cleaned
),
metrics as (
    -- Ranking and gap features
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        is_us,
        is_bangkok,
        rating_share,
        rating_cume_dist,
        count(is_us) over (partition by city) as city_count_is_us,
        lead(rating_share, 1) over (partition by city order by rating_cume_dist, id) as next_rating_share,
        sum(is_bangkok) over (partition by city order by is_us, id rows between unbounded preceding and current row) as cumulative_is_bangkok
    from enriched
),
metrics_suppliers as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over w_part as is_bangkok_bucket,
        sum(next_rating_share) over (partition by city) as city_sum_next_rating_share
    from metrics
    window w_part as (order by is_bangkok, id)
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lag(cumulative_is_bangkok, 1) over (partition by city, country order by rating_cume_dist, id) as prev_cumulative_is_bangkok,
        ntile(4) over (partition by city order by rating_share desc, id) as rating_share_bucket,
        sum(city_count_is_us) over (order by city_sum_next_rating_share, id rows between unbounded preceding and current row) as running_city_count_is_us,
        count(next_rating_share) over (partition by city) as city_count_next_rating_share,
        first_value(next_rating_share, true) over (partition by country, city order by is_bangkok_bucket, id rows between unbounded preceding and current row) as first_known_next_rating_share
    from metrics_suppliers
),
summary as (
    -- Grouped metrics
    select
        city,
        case when city_sum_next_rating_share >= 3 then 'LARGE' else 'SMALL' end as city_sum_next_rating_share_tier,
        sum(case when city = 'London' then city_count_is_us else 0 end) as london_city_count_is_us,
        count(case when country = 'DE' then 1 end) as de_count,
        approx_percentile(rating_share_bucket, array(0.25, 0.5, 0.75)) as rating_share_bucket_quartiles,
        grouping_id() as grouping_level
    from with_windows
    group by grouping sets ((city, case when city_sum_next_rating_share >= 3 then 'LARGE' else 'SMALL' end), (city))
)
select
    de_count,
    rating_share_bucket_quartiles,
    grouping_level
from summary;
