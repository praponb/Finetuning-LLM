select
    order_id,
    case when cost >= 95 then 'PREMIUM' else 'STANDARD' end as cost_tier,
    round(sum(cost) / nullif(count(case when status = 'IN_TRANSIT' then 1 end), 0), 2) as cost_per_in_transit,
    size(collect_set(status)) as num_status,
    approx_percentile(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
    count(case when status = 'IN_TRANSIT' then 1 end) as in_transit_count,
    count(*) filter (where carrier = 'UPS') as ups_rows,
    stddev_samp(cost) as cost_stddev,
    grouping(order_id) as grouping_level
from shipments
group by cube (order_id, case when cost >= 95 then 'PREMIUM' else 'STANDARD' end)
order by order_id desc;
