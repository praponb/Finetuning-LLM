select
    status,
    delivered_month,
    shipment_count,
    carrier_list
from (
    select
        status,
        month(delivered_date) as delivered_month,
        count(*) as shipment_count,
        sort_array(collect_set(carrier)) as carrier_list
    from shipments
    where cost <= 36
    group by status, month(delivered_date)
) as x
where shipment_count >= 6;
