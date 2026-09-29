with metrics as (
    select
        *,
        avg(sqft) over (partition by region, city order by opened_date, id rows between 6 preceding and 1 preceding) as sqft_avg_6_prior,
        lag(sqft, 2) over (partition by region order by sqft, id) as prev_sqft,
        rank() over (partition by city order by sqft nulls first) as sqft_rank
    from stores
),
valid as (
    select *, concat_ws('-', store_name, city) as store_name_city_key
    from metrics
    where opened_date >= to_date('2023-04-23')
),
metrics_stores as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_avg_6_prior,
        prev_sqft,
        sqft_rank,
        store_name_city_key,
        rank() over (order by prev_sqft nulls last) as prev_sqft_rank,
        row_number() over (partition by region order by sqft_avg_6_prior, id) as row_seq,
        ntile(5) over (partition by city order by prev_sqft desc, id) as prev_sqft_bucket,
        count(sqft_rank) over (partition by city, region) as city_count_sqft_rank,
        sum(prev_sqft) over (partition by city) as city_sum_prev_sqft
    from valid
),
scored as (
    select
        *,
        sum(prev_sqft) over (partition by region) as region_sum_prev_sqft,
        first_value(sqft_rank, true) over (partition by region order by sqft_rank, id rows between unbounded preceding and current row) as first_known_sqft_rank
    from metrics_stores
)
select
    region,
    percentile_approx(sqft, 0.9) as sqft_p90,
    avg(region_sum_prev_sqft) as avg_region_sum_prev_sqft,
    round(avg(first_known_sqft_rank), 2) as avg_first_known_sqft_rank
from scored
group by region
having avg_first_known_sqft_rank >= 0
order by sqft_p90 desc;
