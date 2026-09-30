with kept_keys as (
    -- Set intersection on keys
    select order_id, id
    from shipments
    intersect all
    select order_id, id
    from shipments
    where month(delivered_date) in (5, 8, 9)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from shipments s
    inner join kept_keys k on s.order_id = k.order_id and s.id = k.id
)
-- Grouped metrics
select
    date_trunc('quarter', shipped_date) as quarter_start,
    sum(case when carrier = 'Kerry' then cost else 0 end) as kerry_cost,
    count(*) filter (where status = 'IN_TRANSIT') as in_transit_rows,
    sort_array(collect_set(carrier)) as carrier_list,
    stddev_samp(cost) as cost_stddev,
    round(sum(cost) / nullif(count(case when status = 'DELIVERED' then 1 end), 0), 2) as cost_per_delivered
from kept_rows
group by date_trunc('quarter', shipped_date);
