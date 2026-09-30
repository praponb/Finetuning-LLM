with cleaned as (
    select
        *,
        concat_ws(',', status, carrier) status_carrier_key,
        date_sub(delivered_date, 72) delivered_date_minus_72d,
        coalesce(carrier, 'N/A') carrier_clean
    from shipments
    where cost < 64
)
select
    date_trunc('quarter', shipped_date) as quarter_start,
    max(cost) filter (where status = 'RETURNED') as returned_max_cost,
    count(distinct order_id) as distinct_order_ids
from cleaned
group by date_trunc('quarter', shipped_date);
