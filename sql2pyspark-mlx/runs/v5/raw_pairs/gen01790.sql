with base as (
    -- Keep recent, valid rows
    select
        *,
        concat(status, ' - ', method) as status_method,
        case when amount >= 1740 then 'High' when amount >= 260 then 'Medium' else 'Low' end as amount_band,
        coalesce(method, 'UNKNOWN') as resolved_method
    from payments
    where paid_at >= to_date('2023-09-14')
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        method,
        paid_at,
        amount_band,
        count(amount) over w_ordered as last4_count_amount,
        min(amount) over w_ordered as last4_min_amount,
        avg(amount) over w_ordered as last4_avg_amount,
        max(amount) over (order by cast(paid_at as bigint) asc range between 2592000 preceding and current row) as amount_max_30d
    from base
    where order_id is null
    window w_ordered as (partition by order_id order by paid_at asc, id asc rows between 3 preceding and current row)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        method,
        amount_band,
        last4_min_amount,
        last4_avg_amount,
        amount_max_30d,
        max(last4_count_amount) over w_ordered as method_max_last4_count_amount,
        avg(last4_avg_amount) over (partition by method order by last4_min_amount asc, id asc rows between 6 preceding and 1 preceding) as last4_avg_amount_avg_6_prior,
        row_number() over w_running as seq_no
    from with_windows
    window w_ordered as (partition by method), w_running as (order by last4_avg_amount desc, id asc)
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lag(last4_avg_amount) over (partition by method order by seq_no asc, id asc) as prev_last4_avg_amount,
        lag(amount_max_30d) over (partition by method order by seq_no asc, id asc) as prev_amount_max_30d,
        lead(amount_max_30d) over (partition by method order by seq_no asc, id asc) as next_amount_max_30d,
        row_number() over (partition by method order by seq_no asc, id asc) as seq_no_2,
        ntile(4) over (order by amount_max_30d asc, id asc) as amount_max_30d_bucket
    from metrics
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by method order by seq_no_2 desc, id asc) as row_num
        from enriched
    ) x
    where row_num = 1
)
-- Multi-level aggregation
select
    case when last4_min_amount >= 1290 then 'LARGE' else 'SMALL' end as last4_min_amount_tier,
    approx_percentile(last4_avg_amount, array(0.25, 0.5, 0.75)) as last4_avg_amount_quartiles,
    approx_percentile(amount_max_30d_bucket, array(0.25, 0.5, 0.75)) as amount_max_30d_bucket_quartiles,
    round(sum(amount_max_30d_bucket) / nullif(count(case when method = 'bank_transfer' then 1 end), 0), 2) as amount_max_30d_bucket_per_bank_transfer
from deduped
group by case when last4_min_amount >= 1290 then 'LARGE' else 'SMALL' end
having round(sum(amount_max_30d_bucket) / nullif(count(case when method = 'bank_transfer' then 1 end), 0), 2) > 0
order by amount_max_30d_bucket_per_bank_transfer desc nulls first;
