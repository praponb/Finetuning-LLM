with order_summary as (
    select order_id, sort_array(collect_set(status)) status_list, max(shipped_date) latest_shipped_date
    from shipments
    where status in ('RETURNED', 'IN_TRANSIT', 'DELIVERED')
    group by order_id
)
select orders.store_id, order_summary.status_list, order_summary.latest_shipped_date
from orders
inner join order_summary on orders.id = order_summary.order_id
order by order_summary.latest_shipped_date asc
