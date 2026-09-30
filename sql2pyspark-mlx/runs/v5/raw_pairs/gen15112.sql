with cleaned as (
    select *, case when units >= 34 then 'High' when units >= 8 then 'Medium' else 'Low' end as units_tier
    from store_sales
    where sale_date > date '2023-08-24'
      and sale_date between '2023-06-01' and '2023-07-01'
),
deduped as (
    select *, row_number() over (partition by product_id order by sale_date desc, id) as rank_in_group
    from cleaned
    qualify rank_in_group = 1
),
metrics as (
    select
        id,
        units_tier,
        sum(units) over w_recent as last7_sum_units,
        max(units) over w_recent as last7_max_units,
        avg(units) over w_recent as last7_avg_units,
        row_number() over w_ordered as row_seq
    from deduped
    where store_id in (3, 4)
    window w_recent as (partition by store_id, product_id order by revenue, id rows between 6 preceding and current row), w_ordered as (partition by store_id order by sale_date, id)
),
cleaned_store_sales as (
    select
        *,
        row_seq * last7_sum_units as row_seq_times_last7_sum_units,
        case when last7_max_units >= 36 then 'High' when last7_max_units >= 20 then 'Medium' else 'Low' end as last7_max_units_level
    from metrics
    where last7_max_units between 15 and 39
),
metrics_store_sales as (
    select
        *,
        lag(last7_max_units) over (order by row_seq_times_last7_sum_units, id) as prev_last7_max_units,
        lead(last7_max_units) over (order by row_seq_times_last7_sum_units, id) as next_last7_max_units,
        lag(row_seq) over (order by row_seq_times_last7_sum_units, id) as prev_row_seq,
        lead(row_seq) over (order by row_seq_times_last7_sum_units, id) as next_row_seq,
        row_number() over (order by row_seq_times_last7_sum_units, id) as event_seq,
        avg(last7_sum_units) over (order by last7_sum_units, id rows between 2 preceding and 1 preceding) as last7_sum_units_avg_2_prior
    from cleaned_store_sales
),
scored as (
    select
        id,
        units_tier,
        row_seq,
        next_last7_max_units,
        next_row_seq,
        last7_sum_units_avg_2_prior,
        row_number() over (order by next_last7_max_units desc, id) as row_seq_2,
        sum(row_seq_times_last7_sum_units) over (order by prev_last7_max_units, id rows between unbounded preceding and current row) as cumulative_row_seq_times_last7_sum_units
    from metrics_store_sales
),
summary as (
    select
        case when next_last7_max_units >= 44 then 'LARGE' else 'SMALL' end as next_last7_max_units_tier,
        approx_percentile(row_seq, 0.9) as row_seq_p90,
        count(distinct id) as distinct_ids,
        approx_percentile(last7_sum_units_avg_2_prior, 0.5) as last7_sum_units_avg_2_prior_p50,
        percentile_approx(last7_sum_units_avg_2_prior, 0.95) as last7_sum_units_avg_2_prior_p95
    from scored
    group by next_last7_max_units_tier
    having last7_sum_units_avg_2_prior_p50 >= 0
)
select next_last7_max_units_tier
from summary
order by next_last7_max_units_tier
