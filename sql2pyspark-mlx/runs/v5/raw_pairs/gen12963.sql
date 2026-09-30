with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(4) over (order by cost desc, id) as cost_bucket,
        sum(cost) over (partition by carrier) as carrier_sum_cost
    from shipments
)
select
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.status,
    s.carrier_sum_cost,
    (select sum(o2.quantity) from order_items o2 where o2.order_id = s.order_id) as sum_order_item_quantity
from with_windows s
order by id
limit 20;
