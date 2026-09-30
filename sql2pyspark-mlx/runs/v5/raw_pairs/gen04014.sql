with base as (
    select *, length(region) as region_length
    from stores
    where opened_date > date '2023-04-21'
),
deduped as (
    select *
    from base
    qualify row_number() over (partition by city, region order by sqft desc nulls last, id) = 1
),
scored as (
    select
        id,
        opened_date,
        lag(sqft) over (partition by city, region order by region_length, id) as prev_sqft,
        lead(sqft) over (partition by city, region order by region_length, id) as next_sqft,
        lag(region_length) over (partition by city, region order by region_length, id) as prev_region_length,
        lead(region_length) over (partition by city, region order by region_length, id) as next_region_length,
        row_number() over (partition by city, region order by region_length, id) as seq_no
    from deduped
    where length(store_name) > 6
),
metrics as (
    select
        id,
        prev_sqft,
        next_sqft,
        prev_region_length,
        dense_rank() over (order by seq_no desc nulls last) as seq_no_dense_rank,
        avg(next_region_length) over (order by next_region_length, id rows between 5 preceding and current row) as next_region_length_avg_5,
        avg(next_region_length) over (order by prev_region_length, id rows between 2 preceding and current row) as next_region_length_avg_2,
        last_value(prev_region_length, true) over (order by next_region_length, id rows between unbounded preceding and current row) as last_known_prev_region_length
    from scored
)
select
    case when last_known_prev_region_length >= 85 then 'HIGH' else 'LOW' end as last_known_prev_region_length_tier,
    case when prev_region_length >= 69 then 'HIGH' else 'LOW' end as prev_region_length_tier,
    count(*) as cnt,
    max(prev_region_length) as max_prev_region_length,
    count(distinct id) as distinct_ids,
    approx_percentile(next_region_length_avg_2, 0.95) as next_region_length_avg_2_p95,
    count(distinct id) as distinct_ids_2,
    count(distinct id) as distinct_ids_3
from metrics
group by case when last_known_prev_region_length >= 85 then 'HIGH' else 'LOW' end, case when prev_region_length >= 69 then 'HIGH' else 'LOW' end
having max_prev_region_length >= 0
order by distinct_ids_3 nulls last
