with filtered as (
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        substring(carrier, 1, 4) as carrier_prefix,
        datediff(delivered_date, shipped_date) as days_shipped_date_to_delivered_date,
        coalesce(carrier, 'DIRECT') as carrier_filled
    from shipments
    where delivered_date > date '2023-05-26'
      and cost <= 78
),
enriched as (
    select
        id,
        delivered_date,
        carrier_prefix,
        lag(days_shipped_date_to_delivered_date, 1) over w_hist as prev_days_shipped_date_to_delivered_date,
        lag(days_shipped_date_to_delivered_date, 1) over w_hist as prev_days_shipped_date_to_delivered_date_2
    from filtered
    window w_hist as (partition by order_id order by delivered_date, id)
),
cleaned as (
    select
        *,
        datediff(current_date(), delivered_date) as days_since_delivered_date,
        add_months(delivered_date, 12) as delivered_date_plus_months
    from enriched
    where prev_days_shipped_date_to_delivered_date_2 between 28 and 64
),
enriched_shipments as (
    select
        *,
        sum(prev_days_shipped_date_to_delivered_date) over (order by prev_days_shipped_date_to_delivered_date_2, id rows between unbounded preceding and current row) as running_prev_days_shipped_date_to_delivered_date,
        row_number() over (order by delivered_date, id) as event_seq,
        percent_rank() over (order by prev_days_shipped_date_to_delivered_date desc nulls first) as prev_days_shipped_date_to_delivered_date_percent_rank,
        dense_rank() over (order by prev_days_shipped_date_to_delivered_date_2 desc nulls last) as prev_days_shipped_date_to_delivered_date_2_dense_rank
    from cleaned
    where days_since_delivered_date > 78
),
remaining_keys as (
    select id
    from enriched_shipments
    except all
    select id
    from shipments
    where carrier not in ('Kerry', 'FedEx', 'Flash')
),
reconciled as (
    select s.*
    from enriched_shipments s
    inner join remaining_keys k on s.id = k.id
),
with_windows as (
    select
        id,
        prev_days_shipped_date_to_delivered_date,
        prev_days_shipped_date_to_delivered_date_2,
        days_since_delivered_date,
        delivered_date_plus_months,
        prev_days_shipped_date_to_delivered_date_2_dense_rank,
        dense_rank() over (order by prev_days_shipped_date_to_delivered_date_2 desc) as prev_days_shipped_date_to_delivered_date_2_dense_rank_2,
        cume_dist() over (order by event_seq nulls last) as event_seq_cume_dist
    from reconciled
)
select
    id,
    prev_days_shipped_date_to_delivered_date,
    prev_days_shipped_date_to_delivered_date_2,
    days_since_delivered_date,
    delivered_date_plus_months,
    prev_days_shipped_date_to_delivered_date_2_dense_rank_2,
    event_seq_cume_dist
from with_windows
order by prev_days_shipped_date_to_delivered_date_2 desc nulls first;
