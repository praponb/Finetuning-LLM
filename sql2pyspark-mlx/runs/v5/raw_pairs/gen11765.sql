with with_dims as (
    -- Attach orders attributes
    select
        a.*,
        o.order_date,
        o.channel,
        o.shipping_fee
    from shipments a
    left join orders o
        on a.order_id = o.id
    where o.customer_id in (16, 34)
)
select
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.status,
    (select sum(x.cost) from shipments x where x.order_id = s.order_id) as sum_shipment_cost
from with_dims s
order by order_id desc
