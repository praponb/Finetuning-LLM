with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by status, order_id order by delivered_date desc, id) as rank_in_group
        from shipments
    ) x
    where rank_in_group = 1
)
select
    s.id,
    s.order_id,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    s.rank_in_group,
    case when status rlike '^(RETURNED|IN_TRANSIT)$' then 'GROUP_A' else 'CORE' end as status_zone,
    (select sum(o2.quantity) from order_items o2 where o2.order_id = s.order_id) as sum_order_item_quantity
from ranked s
order by order_id nulls first, shipped_date;
