with base as (
    -- Keep recent, valid rows
    select *, lower(method) as method_lower
    from payments
    where paid_at >= date '2023-09-15'
      and amount < 490
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lag(amount, 2) over (partition by order_id order by amount, id) as prev_amount,
        first_value(amount, true) over (partition by status order by amount, id rows between unbounded preceding and current row) as first_known_amount,
        ntile(5) over (partition by order_id order by amount desc, id) as amount_bucket,
        row_number() over (order by amount, id) as seq_no,
        ntile(5) over (partition by order_id order by amount desc, id) as amount_bucket_2
    from base
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        status,
        method_lower,
        prev_amount,
        first_known_amount,
        amount_bucket,
        seq_no,
        amount_bucket_2,
        avg(first_known_amount) over (partition by order_id, method order by amount, id rows between 3 preceding and current row) as last4_avg_first_known_amount,
        max(first_known_amount) over (partition by order_id, method order by amount, id rows between 3 preceding and current row) as last4_max_first_known_amount,
        sum(first_known_amount) over (partition by order_id, method order by amount, id rows between 3 preceding and current row) as last4_sum_first_known_amount
    from scored
),
enriched_payments as (
    -- Running and moving aggregates
    select
        id,
        first_known_amount,
        seq_no,
        amount_bucket_2,
        last4_avg_first_known_amount,
        last4_max_first_known_amount,
        last4_sum_first_known_amount,
        ntile(3) over (partition by order_id, status order by last4_max_first_known_amount, id) as last4_max_first_known_amount_bucket,
        count(last4_sum_first_known_amount) over (partition by order_id, status) as order_id_count_last4_sum_first_known_amount
    from enriched
    where prev_amount between 960 and 1550
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        sum(seq_no) over (order by last4_avg_first_known_amount, id rows between unbounded preceding and current row) as cumulative_seq_no,
        sum(amount_bucket_2) over (order by seq_no, id rows between unbounded preceding and current row) as running_amount_bucket_2,
        ntile(4) over (order by last4_sum_first_known_amount, id) as last4_sum_first_known_amount_bucket,
        ntile(10) over (order by last4_sum_first_known_amount desc, id) as last4_sum_first_known_amount_bucket_2
    from enriched_payments
),
metrics as (
    -- Ranking and gap features
    select
        id,
        first_known_amount,
        seq_no,
        last4_sum_first_known_amount,
        order_id_count_last4_sum_first_known_amount,
        cumulative_seq_no,
        last4_sum_first_known_amount_bucket,
        last4_sum_first_known_amount_bucket_2,
        avg(amount_bucket_2) over (order by first_known_amount, id rows between 3 preceding and current row) as amount_bucket_2_avg_3,
        lag(seq_no, 1) over (order by last4_max_first_known_amount_bucket, id) as prev_seq_no,
        sum(last4_sum_first_known_amount_bucket_2) over (order by amount_bucket_2, id rows between unbounded preceding and current row) as cumulative_last4_sum_first_known_amount_bucket_2
    from with_windows
),
metrics_payments as (
    -- Ranking and gap features
    select
        *,
        min(seq_no) over (order by cumulative_seq_no, id rows between unbounded preceding and current row) as running_min_seq_no,
        count(seq_no) over (order by cumulative_seq_no, id rows between unbounded preceding and current row) as running_count_seq_no,
        max(seq_no) over (order by cumulative_seq_no, id rows between unbounded preceding and current row) as running_max_seq_no,
        sum(seq_no) over (order by cumulative_seq_no, id rows between unbounded preceding and current row) as running_sum_seq_no
    from metrics
)
-- Grouped metrics
select
    cumulative_seq_no,
    max(running_count_seq_no) as max_running_count_seq_no,
    avg(cumulative_seq_no) as avg_cumulative_seq_no
from metrics_payments
group by cube (cumulative_seq_no)
having avg(cumulative_seq_no) > 0
order by max_running_count_seq_no desc;
