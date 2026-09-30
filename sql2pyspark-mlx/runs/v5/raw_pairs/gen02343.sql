with matched_keys as (
    -- Set difference on keys
    select id, order_id
    from shipments
    except all
    select id, order_id
    from shipments
    where delivered_date >= '2023-06-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from shipments s
    inner join matched_keys k on s.id = k.id and s.order_id = k.order_id
)
select
    s.id,
    s.order_id,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    case when status rlike '^(IN_TRANSIT|DELIVERED)$' then 'PRIORITY' else 'GROUP_B' end as status_zone,
    (select min(x.quantity) from order_items x where x.order_id = s.order_id) as min_order_item_quantity
from kept_rows s
order by delivered_date desc nulls last
