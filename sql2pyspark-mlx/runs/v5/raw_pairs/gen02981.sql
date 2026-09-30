with base as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        initcap(trim(status)) as status_title
    from payments
    where order_id is not null
      and status = 'REFUNDED'
),
with_windows as (
    select
        *,
        sum(amount) over w_hist as last7_sum_amount,
        avg(amount) over w_hist as last7_avg_amount,
        max(amount) over w_hist as last7_max_amount,
        min(amount) over w_hist as last7_min_amount,
        round(amount / sum(amount) over (partition by status), 4) as amount_share
    from base
    window w_hist as (partition by order_id, status order by paid_at, id rows between 6 preceding and current row)
),
with_windows_payments as (
    select
        *,
        ntile(10) over (partition by order_id order by last7_avg_amount, id) as last7_avg_amount_bucket,
        lead(last7_max_amount, 1) over w_running as next_last7_max_amount
    from with_windows
    where last7_max_amount >= 1500
    window w_running as (order by last7_max_amount, id)
),
metrics as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        status_title,
        last7_sum_amount,
        amount_share,
        last7_avg_amount_bucket,
        next_last7_max_amount,
        lead(last7_min_amount) over w_ordered as next_last7_min_amount,
        lag(last7_min_amount) over w_ordered as prev_last7_min_amount,
        lag(last7_avg_amount_bucket) over w_ordered as prev_last7_avg_amount_bucket,
        lead(last7_avg_amount_bucket) over w_ordered as next_last7_avg_amount_bucket,
        row_number() over w_ordered as seq_no,
        first_value(last7_max_amount, true) over (partition by status order by amount_share, id rows between unbounded preceding and current row) as first_known_last7_max_amount
    from with_windows_payments
    window w_ordered as (partition by order_id order by next_last7_max_amount, id)
),
filtered as (
    select
        *,
        round(seq_no * 100, 2) as seq_no_scaled,
        case when seq_no >= 8 then 'High' when seq_no >= 4 then 'Medium' else 'Low' end as seq_no_tier
    from metrics
    where paid_at > to_timestamp('2023-07-20 22:00:00')
      and amount < 1250
),
summary as (
    select
        date_trunc('quarter', paid_at) as quarter_start,
        min(paid_at) as started_at,
        size(collect_set(status)) as num_status,
        max(paid_at) as ended_at
    from filtered
    group by 1
    having size(collect_set(status)) > 1
)
select
    quarter_start,
    started_at,
    num_status,
    ended_at
from summary
order by quarter_start desc, started_at desc
