with base as (
    select
        *,
        date_format(shipped_date, 'MMM yyyy') as shipped_date_label,
        coalesce(carrier, 'DIRECT') as resolved_carrier
    from shipments
    where carrier is not null
      and carrier is distinct from 'FedEx'
),
joined as (
    select
        s.id,
        s.order_id,
        s.carrier,
        s.status,
        s.resolved_carrier,
        p.status as order_status
    from base s
    left join orders p
        on s.order_id <=> p.id
),
with_dims as (
    select
        src.id,
        src.order_id,
        src.carrier,
        src.status,
        src.resolved_carrier,
        src.order_status,
        o.status as order_status_2,
        o.shipping_fee,
        o.order_date
    from joined src
    inner join orders o
        on src.order_id <=> o.id
),
scored as (
    select
        id,
        order_id,
        carrier,
        resolved_carrier,
        shipping_fee,
        min(shipping_fee) over w_ordered as status_min_shipping_fee,
        row_number() over (partition by order_status_2, order_id order by order_date, id) as event_seq,
        sum(shipping_fee) over (partition by status order by order_date, id rows between unbounded preceding and current row) as running_shipping_fee,
        avg(shipping_fee) over w_recent as status_avg_shipping_fee,
        sum(shipping_fee) over w_running as cumulative_shipping_fee
    from with_dims
    window w_ordered as (partition by status, order_status), w_recent as (partition by status, order_status_2), w_running as (partition by order_status order by order_date, id rows between unbounded preceding and current row)
)
select
    carrier,
    approx_percentile(event_seq, 0.5) as event_seq_p50,
    max(status_avg_shipping_fee) filter (where carrier = 'FedEx') as fedex_max_status_avg_shipping_fee,
    count(*) filter (where carrier = 'UPS') as ups_rows,
    min(event_seq) as min_event_seq,
    sort_array(collect_set(carrier)) as carrier_list
from scored
group by 1
order by ups_rows desc nulls last;
