with with_windows as (
    -- Running and moving aggregates
    select
        *,
        lead(sqft, 1, 0) over (partition by city order by sqft asc, id asc) as next_sqft,
        row_number() over (partition by region order by sqft desc, id asc) as seq_no,
        avg(sqft) over (partition by region order by opened_date asc, id asc rows between 3 preceding and 1 preceding) as sqft_avg_3_prior,
        dense_rank() over (partition by region, city order by sqft asc nulls last) as sqft_dense_rank
    from stores
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        next_sqft,
        seq_no,
        sqft_avg_3_prior,
        sqft_dense_rank,
        date_sub(opened_date, 85) as opened_date_minus_85d
    from with_windows
    where opened_date > to_date('2023-05-17')
      and opened_date > '2023-08-01'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        sum(sqft) over (order by sqft_dense_rank asc, id asc rows between unbounded preceding and current row) as cumulative_sqft,
        row_number() over (partition by city order by sqft desc, id asc) as row_seq,
        stddev_samp(next_sqft) over (partition by city, region) as city_stddev_samp_next_sqft,
        cume_dist() over (partition by city, region order by next_sqft asc nulls first) as next_sqft_cume_dist,
        dense_rank() over (order by seq_no asc) as seq_no_dense_rank
    from cleaned
)
-- Native SQL PIVOT
select *
from (
    select
        region,
        city,
        seq_no
    from scored
) src
pivot (
    avg(seq_no)
    for city in ('Berlin', 'Tokyo')
);
