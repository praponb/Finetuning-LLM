with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        date_format(sale_date, 'yyyy-MM-dd') as sale_date_label
    from store_sales
    where sale_date >= date '2023-03-15'
      and units <> 20
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        sale_date,
        units,
        ntile(4) over (partition by product_id, store_id order by units, id) as units_bucket,
        row_number() over (partition by store_id, product_id order by units desc, id) as row_seq
    from valid
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(row_seq) over (order by units, id rows between unbounded preceding and current row) as running_row_seq,
        row_number() over w_recent as row_seq_2,
        sum(row_seq) over (order by row_seq, id rows between unbounded preceding and current row) as cumulative_row_seq,
        avg(units) over (order by units_bucket, id rows between 3 preceding and 1 preceding) as units_avg_3_prior
    from enriched
    window w_recent as (order by units_bucket, id)
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(units_avg_3_prior, 0) as units_avg_3_prior_filled,
        round(running_row_seq / units_bucket, 2) as running_row_seq_per_units_bucket
    from metrics
    where sale_date >= date '2023-06-12'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        units,
        row_seq,
        running_row_seq,
        row_seq_2,
        cumulative_row_seq,
        units_avg_3_prior_filled,
        running_row_seq_per_units_bucket,
        units_avg_3_prior * row_seq_2 as units_avg_3_prior_times_row_seq_2,
        least(row_seq_2, units_bucket) as least_row_seq_2_units_bucket
    from cleaned
    where row_seq_2 > 5
),
scored as (
    -- Ranking and gap features
    select
        id,
        row_seq,
        running_row_seq,
        row_seq_2,
        running_row_seq_per_units_bucket,
        first_value(running_row_seq) ignore nulls over (order by units_avg_3_prior_times_row_seq_2, id rows between unbounded preceding and current row) as first_known_running_row_seq,
        avg(running_row_seq_per_units_bucket) over (order by least_row_seq_2_units_bucket, id rows between 6 preceding and current row) as running_row_seq_per_units_bucket_avg_6,
        percent_rank() over (order by running_row_seq_per_units_bucket nulls first) as running_row_seq_per_units_bucket_percent_rank
    from filtered
),
enriched_store_sales as (
    -- Ranking and gap features
    select
        *,
        avg(row_seq_2) over w_ordered as row_seq_2_avg_6,
        first_value(running_row_seq) ignore nulls over (order by row_seq_2, id rows between unbounded preceding and current row) as first_known_running_row_seq_2
    from scored
    where row_seq_2 <> 3
    window w_ordered as (order by row_seq_2, id rows between 6 preceding and current row)
)
-- Grouped metrics
select
    case when row_seq >= 9 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    approx_percentile(row_seq, 0.9) as row_seq_p90,
    percentile_approx(first_known_running_row_seq_2, array(0.25, 0.5, 0.75)) as first_known_running_row_seq_2_quartiles,
    round(avg(row_seq_2), 2) as avg_row_seq_2,
    count(*) as event_count,
    percentile_approx(row_seq_2, array(0.25, 0.5, 0.75)) as row_seq_2_quartiles,
    count(distinct id) as distinct_ids
from enriched_store_sales
group by row_seq_tier
order by row_seq_tier nulls first;
