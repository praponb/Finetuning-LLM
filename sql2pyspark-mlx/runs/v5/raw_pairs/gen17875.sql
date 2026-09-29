with remaining_keys as (
    select city, id
    from stores
    except
    select city, id
    from stores
    where city in ('Bangkok', 'Berlin')
),
verified as (
    select s.*
    from stores s
    join remaining_keys k on s.city = k.city and s.id = k.id
),
metrics as (
    select
        *,
        avg(sqft) over (partition by city) city_avg_sqft,
        rank() over (partition by city order by sqft desc) sqft_rank,
        cume_dist() over (partition by city order by sqft desc) sqft_cume_dist
    from verified
),
remaining_keys_2 as (
    select region, id
    from metrics
    intersect
    select region, id
    from stores
    where region in ('East', 'Central', 'West')
),
kept_rows as (
    select s.*
    from metrics s
    join remaining_keys_2 k on s.region = k.region and s.id = k.id
),
enriched as (
    select
        store_name,
        city,
        opened_date,
        city_avg_sqft,
        sqft_cume_dist,
        dense_rank() over (partition by city, region order by sqft_rank desc) sqft_rank_dense_rank,
        round(sqft / sum(sqft) over (partition by city), 4) sqft_share
    from kept_rows
),
summary as (
    select
        city,
        count(*) filter (where city = 'Bangkok') as bangkok_rows,
        round(avg(sqft_cume_dist), 2) as avg_sqft_cume_dist,
        sum(sqft_cume_dist) as total_sqft_cume_dist
    from enriched
    group by city
    having round(avg(sqft_cume_dist), 2) > 0
)
select
    city,
    bangkok_rows,
    avg_sqft_cume_dist,
    total_sqft_cume_dist,
    case when city rlike '^(New York|Berlin)$' then 'CORE' when city rlike '^(Bangkok|Chiang Mai)$' then 'OTHER_ZONE' else 'EXTENDED' end as city_zone
from summary
order by city_zone desc nulls last
