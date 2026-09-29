with enriched as (
    select
        id,
        order_id,
        carrier,
        avg(cost) over (partition by order_id order by delivered_date, id rows between 6 preceding and 1 preceding) as cost_avg_6_prior,
        row_number() over (partition by status order by delivered_date, id) as event_seq
    from shipments
    where carrier in ('Kerry', 'UPS', 'FedEx')
),
latest as (
    select *, row_number() over (partition by carrier order by event_seq desc, id) as row_num
    from enriched
    qualify row_num = 1
),
deduped as (
    select
        id,
        event_seq,
        row_num
    from latest
    qualify row_number() over (partition by order_id, carrier order by event_seq nulls first, id) = 1
),
with_windows as (
    select
        *,
        count(event_seq) over w_recent as last4_count_event_seq,
        avg(event_seq) over w_recent as last4_avg_event_seq,
        sum(event_seq) over w_recent as last4_sum_event_seq,
        lag(event_seq, 1) over w_part as prev_event_seq
    from deduped
    window w_recent as (order by event_seq, id rows between 3 preceding and current row), w_part as (order by row_num, id)
),
scored as (
    select
        id,
        last4_count_event_seq,
        lead(last4_sum_event_seq, 1) over (order by last4_avg_event_seq, id) as next_last4_sum_event_seq,
        ntile(3) over (order by last4_avg_event_seq, id) as last4_avg_event_seq_bucket,
        row_number() over (order by last4_count_event_seq desc, id) as row_seq
    from with_windows
),
cleaned as (
    select
        *,
        last4_avg_event_seq_bucket - next_last4_sum_event_seq as last4_avg_event_seq_bucket_minus_next_last4_sum_event_seq,
        case when next_last4_sum_event_seq >= 35 then 'High' when next_last4_sum_event_seq >= 20 then 'Medium' else 'Low' end as next_last4_sum_event_seq_band
    from scored
    where last4_avg_event_seq_bucket != 5
),
metrics as (
    select
        *,
        cume_dist() over (order by last4_count_event_seq desc) as last4_count_event_seq_cume_dist,
        last_value(last4_avg_event_seq_bucket) ignore nulls over (order by last4_count_event_seq, id rows between unbounded preceding and current row) as last_known_last4_avg_event_seq_bucket,
        sum(row_seq) over (order by row_seq, id rows between unbounded preceding and current row) as cumulative_row_seq
    from cleaned
)
select *
from (
    select
        id,
        last4_avg_event_seq_bucket_minus_next_last4_sum_event_seq,
        next_last4_sum_event_seq,
        last4_count_event_seq_cume_dist
    from metrics
) src
unpivot include nulls (
    reading for metric in (last4_avg_event_seq_bucket_minus_next_last4_sum_event_seq, next_last4_sum_event_seq, last4_count_event_seq_cume_dist)
) u
