with with_latest as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.shipment_cost,
        top1.carrier
    from orders s
    left join lateral (
            select c.cost as shipment_cost, c.carrier
            from shipments c
            where c.order_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    customer_id,
    order_date,
    status,
    channel,
    amount,
    shipping_fee,
    carrier
from with_latest
order by carrier;
