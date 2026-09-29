with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat(city, '/', region) as city_region,
        coalesce(region, 'UNKNOWN') as region_clean
    from stores
    where opened_date > date '2023-03-25'
      and region is not null
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.order_date >= s.opened_date
        )
),
enriched as (
    -- Ranking and gap features
    select
        id,
        store_name,
        city,
        region,
        sqft,
        city_region,
        sum(sqft) over (partition by city) as city_sum_sqft,
        last_value(region) ignore nulls over (partition by city order by opened_date, id rows between unbounded preceding and current row) as last_known_region,
        row_number() over (partition by region order by sqft, id) as row_seq
    from eligible
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        max(city_sum_sqft) over w_ordered as running_max_city_sum_sqft,
        min(city_sum_sqft) over w_ordered as running_min_city_sum_sqft,
        count(city_sum_sqft) over w_ordered as running_count_city_sum_sqft,
        sum(city_sum_sqft) over w_ordered as running_sum_city_sum_sqft
    from enriched
    window w_ordered as (partition by region order by sqft, id rows between unbounded preceding and current row)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        city,
        region,
        city_region,
        last_known_region,
        running_max_city_sum_sqft,
        running_min_city_sum_sqft,
        ntile(10) over (partition by region order by row_seq desc, id) as row_seq_bucket,
        round(running_min_city_sum_sqft / sum(running_min_city_sum_sqft) over (partition by city), 4) as running_min_city_sum_sqft_share
    from with_windows
),
daily as (
    -- Aggregate with conditional metrics
    select
        case when running_min_city_sum_sqft_share >= 0.63 then 'HIGH' else 'LOW' end as running_min_city_sum_sqft_share_tier,
        round(avg(running_min_city_sum_sqft), 2) as avg_running_min_city_sum_sqft,
        round(avg(running_min_city_sum_sqft_share), 2) as avg_running_min_city_sum_sqft_share,
        count(*) filter (where city = 'Berlin') as berlin_rows
    from scored
    group by case when running_min_city_sum_sqft_share >= 0.63 then 'HIGH' else 'LOW' end
)
select
    running_min_city_sum_sqft_share_tier,
    avg_running_min_city_sum_sqft,
    berlin_rows
from daily
order by running_min_city_sum_sqft_share_tier desc;
