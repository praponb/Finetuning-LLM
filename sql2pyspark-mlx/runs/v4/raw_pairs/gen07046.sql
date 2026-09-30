with base as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(delivered_date, shipped_date) as days_shipped_date_to_delivered_date
    from shipments
    where shipped_date > date '2023-06-26'
)
-- Attach orders attributes
select
    a.*,
    o.channel,
    o.amount,
    o.shipping_fee
from base a
left join orders o
    on a.order_id = o.id;
