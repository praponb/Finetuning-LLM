with with_windows as (
    select
        id,
        customer_id,
        plan,
        row_number() over w_part as row_seq,
        ntile(4) over w_part_2 as monthly_fee_bucket
    from subscriptions
    window w_part as (partition by plan order by start_date desc, id), w_part_2 as (partition by customer_id, status order by monthly_fee, id)
),
cleaned as (
    select *, cast(monthly_fee_bucket as double) as monthly_fee_bucket_str
    from with_windows
    where customer_id is not null
),
with_windows_subscriptions as (
    select
        id,
        customer_id,
        plan,
        row_seq,
        monthly_fee_bucket_str,
        lag(row_seq) over (partition by plan order by row_seq, id) as prev_row_seq,
        lead(row_seq) over (partition by plan order by row_seq, id) as next_row_seq,
        lead(monthly_fee_bucket) over (partition by plan order by row_seq, id) as next_monthly_fee_bucket,
        lag(monthly_fee_bucket) over (partition by plan order by row_seq, id) as prev_monthly_fee_bucket,
        row_number() over (partition by plan order by row_seq, id) as event_seq,
        avg(monthly_fee_bucket_str) over (partition by customer_id, plan order by monthly_fee_bucket_str, id rows between 5 preceding and current row) as monthly_fee_bucket_str_avg_5
    from cleaned
),
enriched as (
    select
        *,
        lead(next_row_seq) over w_running as next_next_row_seq,
        lag(next_row_seq) over w_running as prev_next_row_seq,
        lead(row_seq) over w_running as next_row_seq_2,
        lag(row_seq) over w_running as prev_row_seq_2,
        row_number() over w_running as row_seq_2,
        round(prev_row_seq / sum(prev_row_seq) over (partition by customer_id), 4) as prev_row_seq_share
    from with_windows_subscriptions
    window w_running as (partition by customer_id, plan order by prev_monthly_fee_bucket, id)
),
base as (
    select
        *,
        round(prev_next_row_seq, 0) as prev_next_row_seq_rounded,
        round(next_next_row_seq, 1) as next_next_row_seq_rounded
    from enriched
    where customer_id is not null
)
select
    c.country,
    c.segment,
    e.plan,
    count(*) as rows_total,
    sum(e.prev_next_row_seq_rounded) as total_prev_next_row_seq_rounded,
    min(e.row_seq) as min_row_seq
from base e
inner join customers c on e.customer_id = c.id
group by grouping sets ((c.country, c.segment, e.plan), (c.country));
