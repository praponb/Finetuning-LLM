with enriched as (
    select
        a.*,
        b.discount,
        b.order_date,
        b.shipping_fee
    from order_items a
    left join orders b
        on a.order_id = b.id
)
select
    s.id,
    s.order_id,
    s.quantity,
    s.unit_price,
    s.discount,
    (select avg(x.amount) from payments x where x.order_id = s.order_id) as avg_payment_amount
from enriched s
order by unit_price desc
