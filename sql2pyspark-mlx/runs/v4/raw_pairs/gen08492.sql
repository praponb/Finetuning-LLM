with valid as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        region,
        opened_date,
        sqft,
        length(store_name) as store_name_length,
        coalesce(region, 'UNKNOWN') as region_filled
    from stores
    where region is not null
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        store_name,
        region,
        opened_date,
        sqft,
        store_name_length,
        region_filled,
        last_value(sqft, true) over (partition by region order by sqft, id rows between unbounded preceding and current row) as last_known_sqft,
        cume_dist() over (partition by region order by store_name_length) as store_name_length_cume_dist,
        row_number() over (partition by region order by sqft, id) as seq_no,
        avg(store_name_length) over (partition by region order by opened_date, id rows between 5 preceding and current row) as store_name_length_avg_5,
        lag(store_name_length, 2) over (partition by region order by sqft, id) as prev_store_name_length
    from valid
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by region order by store_name_length, id) as seq_no_2,
        row_number() over w_grp as event_seq,
        stddev_samp(store_name_length_avg_5) over (partition by region) as region_stddev_samp_store_name_length_avg_5
    from enriched
    window w_grp as (partition by region order by store_name_length_avg_5, id)
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, seq_no / nullif(store_name_length_avg_5, 0) as seq_no_per_store_name_length_avg_5
    from metrics
    where region is not null
      and seq_no_2 is not null
),
totals as (
    -- Aggregate with conditional metrics
    select
        date_trunc('year', opened_date) as year_start,
        case when seq_no_2 >= 9 then 'PREMIUM' else 'STANDARD' end as seq_no_2_tier,
        approx_percentile(region_stddev_samp_store_name_length_avg_5, array(0.25, 0.5, 0.75)) as region_stddev_samp_store_name_length_avg_5_quartiles,
        count(*) as cnt,
        bit_and(sqft) as sqft_bit_and,
        bit_and(sqft) as sqft_bit_and_2,
        round(sum(sqft) / nullif(count(case when region = 'North' then 1 end), 0), 2) as sqft_per_north
    from base
    group by date_trunc('year', opened_date), case when seq_no_2 >= 9 then 'PREMIUM' else 'STANDARD' end
    having sqft_bit_and_2 >= 1
)
select
    year_start,
    seq_no_2_tier,
    region_stddev_samp_store_name_length_avg_5_quartiles,
    sqft_bit_and,
    sqft_bit_and_2,
    sqft_per_north
from totals
order by sqft_bit_and desc;
