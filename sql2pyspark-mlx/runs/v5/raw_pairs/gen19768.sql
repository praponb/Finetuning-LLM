select
    case when carrier in ('UPS', 'FedEx') then 'core' else 'other' end as carrier_group,
    round((100 * sum(case when status = 'LOST' then 1 else 0 end)) / count(*), 1) as pct_lost,
    sort_array(collect_set(carrier)) as carrier_list,
    concat_ws(', ', sort_array(collect_set(status))) as status_values
from shipments
group by case when carrier in ('UPS', 'FedEx') then 'core' else 'other' end;
