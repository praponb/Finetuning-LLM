with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        quantity,
        round(quantity, 0) as quantity_rounded
    from order_items
    where quantity is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        quantity,
        quantity_rounded,
        last_value(quantity_rounded, true) over (partition by product_id order by quantity_rounded, id rows between unbounded preceding and current row) as last_known_quantity_rounded,
        row_number() over w_grp as seq_no,
        ntile(5) over (partition by product_id order by quantity_rounded desc, id) as quantity_rounded_bucket,
        ntile(4) over w_grp as quantity_rounded_bucket_2,
        avg(quantity) over w_running as quantity_avg_5_prior
    from filtered
    where product_id in (20, 30, 36, 39)
    window w_grp as (partition by product_id order by quantity_rounded, id), w_running as (partition by product_id order by quantity, id rows between 5 preceding and 1 preceding)
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        last_known_quantity_rounded,
        seq_no,
        quantity_rounded_bucket_2,
        quantity_avg_5_prior,
        sum(quantity_avg_5_prior) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) as running_quantity_avg_5_prior,
        cume_dist() over (partition by product_id order by quantity_rounded_bucket desc) as quantity_rounded_bucket_cume_dist,
        row_number() over (partition by product_id order by quantity_avg_5_prior, id) as row_seq,
        percent_rank() over (order by quantity_avg_5_prior) as quantity_avg_5_prior_percent_rank
    from enriched
),
cleaned as (
    -- Basic cleansing and defaults
    select *, running_quantity_avg_5_prior - quantity_avg_5_prior_percent_rank as running_quantity_avg_5_prior_minus_quantity_avg_5_prior_percent_rank
    from with_windows
    where quantity_avg_5_prior_percent_rank <= 0.47
)
-- Grouped metrics
select
    seq_no,
    approx_percentile(quantity_rounded_bucket_cume_dist, 0.95) as quantity_rounded_bucket_cume_dist_p95,
    bit_and(quantity) as quantity_bit_and,
    avg(running_quantity_avg_5_prior) as avg_running_quantity_avg_5_prior
from cleaned
group by 1;
