with valid as (
    -- Keep recent, valid rows
    select
        id,
        amount,
        paid_at,
        status,
        ceil(amount) as amount_whole,
        coalesce(method, 'DIRECT') as method_clean
    from payments
    where paid_at >= to_date('2023-06-06')
      and order_id is not null
      and paid_at >= '2025-10-27'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over (partition by status order by amount desc, id) as amount_bucket,
        avg(amount_whole) over (partition by status order by amount, id rows between 6 preceding and 1 preceding) as amount_whole_avg_6_prior,
        ntile(4) over w_recent as amount_whole_bucket,
        ntile(5) over w_hist as amount_whole_bucket_2
    from valid
    window w_recent as (partition by status order by amount_whole desc, id), w_hist as (order by amount_whole, id)
),
scored as (
    -- Ranking and gap features
    select
        id,
        amount,
        paid_at,
        status,
        amount_whole,
        method_clean,
        amount_bucket,
        amount_whole_avg_6_prior,
        amount_whole_bucket,
        amount_whole_bucket_2,
        sum(amount_whole_avg_6_prior) over (order by amount_whole, id rows between unbounded preceding and current row) as running_amount_whole_avg_6_prior,
        min(amount_whole_avg_6_prior) over (partition by status) as status_min_amount_whole_avg_6_prior,
        avg(amount_whole) over (partition by status order by amount_whole_bucket_2, id rows between 2 preceding and 1 preceding) as amount_whole_avg_2_prior,
        sum(amount) over (partition by status order by amount_whole_bucket_2, id rows between unbounded preceding and current row) as running_amount
    from with_windows
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(running_amount) ignore nulls over (partition by status order by running_amount, id rows between unbounded preceding and current row) as last_known_running_amount,
        sum(status_min_amount_whole_avg_6_prior) over (partition by status order by cast(paid_at as bigint) range between 7776000 preceding and current row) as status_min_amount_whole_avg_6_prior_sum_90d,
        rank() over w_part as running_amount_whole_avg_6_prior_rank,
        round(amount_whole_bucket_2 / sum(amount_whole_bucket_2) over (partition by status), 4) as amount_whole_bucket_2_share
    from scored
    window w_part as (order by running_amount_whole_avg_6_prior desc nulls last)
),
enriched_payments as (
    -- Window metrics over several different partitions
    select
        *,
        lag(amount_bucket, 1) over (order by amount_whole_bucket, id) as prev_amount_bucket,
        ntile(4) over (order by running_amount_whole_avg_6_prior_rank desc, id) as running_amount_whole_avg_6_prior_rank_bucket,
        sum(status_min_amount_whole_avg_6_prior_sum_90d) over (partition by status) as status_sum_status_min_amount_whole_avg_6_prior_sum_90d,
        rank() over (partition by status order by amount_whole_avg_6_prior nulls last) as amount_whole_avg_6_prior_rank
    from enriched
)
select
    id,
    paid_at,
    status,
    method_clean,
    amount_bucket,
    amount_whole_bucket,
    amount_whole_bucket_2,
    status_min_amount_whole_avg_6_prior_sum_90d,
    running_amount_whole_avg_6_prior_rank,
    case when status rlike '^(FAILED|SUCCESS)$' then 'PRIORITY' else 'OTHER_ZONE' end as status_zone
from enriched_payments
