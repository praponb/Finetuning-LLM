with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        length(carrier) as carrier_length,
        coalesce(carrier, 'DIRECT') as carrier_clean
    from shipments
    where shipped_date > date '2023-06-23'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by order_id order by carrier_length nulls first) as carrier_length_cume_dist,
        row_number() over (order by shipped_date desc, id) as seq_no,
        ntile(10) over w_grp as cost_bucket
    from cleaned
    window w_grp as (partition by order_id order by cost desc, id)
),
with_windows_shipments as (
    -- Ranking and gap features
    select
        id,
        carrier,
        shipped_date,
        cost,
        seq_no,
        avg(seq_no) over (partition by order_id order by carrier_length, id rows between 3 preceding and 1 preceding) as seq_no_avg_3_prior,
        sum(cost) over (partition by carrier) as carrier_sum_cost,
        row_number() over (partition by carrier, order_id order by delivered_date desc, id) as seq_no_2,
        ntile(3) over w_hist as cost_bucket_bucket
    from with_windows
    window w_hist as (order by cost_bucket, id)
),
with_windows_shipments_2 as (
    -- Running and moving aggregates
    select
        *,
        ntile(5) over (order by cost_bucket_bucket desc, id) as cost_bucket_bucket_bucket,
        ntile(4) over (order by seq_no_2, id) as seq_no_2_bucket
    from with_windows_shipments
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        carrier,
        seq_no,
        seq_no_avg_3_prior,
        cost_bucket_bucket_bucket,
        seq_no_2_bucket
    from (
        select *, row_number() over (partition by carrier order by cost_bucket_bucket desc nulls first, id) as rn
        from with_windows_shipments_2
    ) x
    where rn = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(seq_no_avg_3_prior) over w_hist as running_sum_seq_no_avg_3_prior,
        min(seq_no_avg_3_prior) over w_hist as running_min_seq_no_avg_3_prior,
        avg(seq_no_avg_3_prior) over w_hist as running_avg_seq_no_avg_3_prior,
        max(seq_no_avg_3_prior) over w_hist as running_max_seq_no_avg_3_prior
    from deduped
    window w_hist as (partition by carrier order by cost_bucket_bucket_bucket, id rows between unbounded preceding and current row)
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by carrier order by running_sum_seq_no_avg_3_prior desc, id) as latest_rank
    from metrics
    qualify latest_rank = 1
)
select
    id,
    seq_no,
    seq_no_avg_3_prior,
    running_avg_seq_no_avg_3_prior,
    running_max_seq_no_avg_3_prior,
    latest_rank
from ranked;
