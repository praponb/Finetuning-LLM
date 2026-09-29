with filtered as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        case when status = 'LOST' then 1 else 0 end as is_lost,
        date_format(shipped_date, 'yyyy-MM') as shipped_date_label,
        coalesce(carrier, 'DIRECT') as carrier_clean
    from shipments
    where carrier is not distinct from 'DHL'
),
enriched as (
    select
        *,
        lead(is_lost) over w_part as next_is_lost,
        lead(cost) over w_part as next_cost,
        lag(cost) over w_part as prev_cost,
        row_number() over w_part as event_seq,
        dense_rank() over (partition by carrier order by is_lost desc nulls first) as is_lost_dense_rank
    from filtered
    where is_lost >= 45
    window w_part as (partition by carrier, order_id order by delivered_date, id)
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by order_id, carrier order by event_seq desc nulls first, id) as latest_rank
        from enriched
    ) t
    where latest_rank = 1
),
deduped_shipments as (
    select *, row_number() over (partition by carrier order by is_lost_dense_rank desc nulls last, id) as row_num
    from deduped
    qualify row_num = 1
),
deduped_shipments_2 as (
    select *, row_number() over (partition by order_id order by row_num, id) as dedup_rank
    from deduped_shipments
    qualify dedup_rank = 1
),
scored as (
    select
        *,
        max(event_seq) over w_part as last7_max_event_seq,
        count(event_seq) over w_part as last7_count_event_seq,
        sum(event_seq) over w_part as last7_sum_event_seq,
        avg(event_seq) over w_part as last7_avg_event_seq,
        ntile(3) over (partition by order_id order by next_cost desc, id) as next_cost_bucket
    from deduped_shipments_2
    window w_part as (partition by carrier order by is_lost_dense_rank, id rows between 6 preceding and current row)
)
select
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    is_lost,
    shipped_date_label,
    next_cost,
    prev_cost
from scored;
