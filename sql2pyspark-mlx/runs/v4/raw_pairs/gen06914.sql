with joined as (
    select
        s.*,
        d.order_date,
        d.shipping_fee
    from shipments s
    left join orders d
        on s.order_id = d.id
)
select
    id,
    shipped_date,
    delivered_date,
    cost,
    status,
    order_date,
    shipping_fee,
    case when status rlike '^(LOST|RETURNED)$' then 'CORE' when status rlike '^(IN_TRANSIT|DELIVERED)$' then 'EXTENDED' else 'OTHER_ZONE' end as status_zone
from joined
order by id, cost desc
