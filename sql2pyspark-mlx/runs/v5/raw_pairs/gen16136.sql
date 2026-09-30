with cleaned as (
    select
        *,
        date_add(shipped_date, 11) as shipped_date_plus_11d,
        coalesce(carrier, 'DIRECT') as resolved_carrier
    from shipments
    where carrier is not null
),
metrics as (
    select
        *,
        lead(cost) over (partition by carrier order by delivered_date, id) as next_cost,
        lag(cost) over (partition by carrier order by delivered_date, id) as prev_cost,
        row_number() over (partition by carrier order by delivered_date, id) as event_seq,
        lead(cost, 2) over (partition by order_id order by shipped_date_plus_11d, id) as next_cost_2
    from cleaned
),
enriched as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        status,
        resolved_carrier,
        next_cost,
        prev_cost,
        event_seq,
        next_cost_2,
        percent_rank() over (order by next_cost_2) as next_cost_2_percent_rank,
        lead(next_cost, 2) over w_ordered as next_next_cost,
        row_number() over (partition by carrier, status order by next_cost_2 desc, id) as row_seq
    from metrics
    window w_ordered as (partition by carrier order by event_seq, id)
),
metrics_shipments as (
    select
        *,
        first_value(carrier, true) over w_ordered as first_known_carrier,
        avg(row_seq) over w_grp as status_avg_row_seq,
        last_value(prev_cost) ignore nulls over (partition by carrier, status order by next_next_cost, id rows between unbounded preceding and current row) as last_known_prev_cost
    from enriched
    where prev_cost < 40
    window w_ordered as (partition by order_id order by next_next_cost, id rows between unbounded preceding and current row), w_grp as (partition by status)
),
scored as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        status,
        next_cost,
        event_seq,
        next_cost_2,
        row_seq,
        first_known_carrier,
        status_avg_row_seq,
        last_known_prev_cost,
        sum(status_avg_row_seq) over (partition by order_id order by next_next_cost, id rows between unbounded preceding and current row) as running_status_avg_row_seq,
        row_number() over (order by next_cost_2 desc, id) as row_seq_2,
        ntile(4) over (partition by carrier order by next_cost_2 desc, id) as next_cost_2_bucket
    from metrics_shipments
)
select *
from (
    select
        order_id,
        status,
        row_seq_2
    from scored
) src
pivot (
    max(row_seq_2)
    for status in ('LOST', 'DELIVERED')
)
