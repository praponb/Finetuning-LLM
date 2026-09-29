with valid as (
    -- Basic cleansing and defaults
    select *, cast(discount as int) discount_int
    from orders
    where channel is not null
      and store_id = 22
)
-- Join a dimension and aggregate
select
    d.city,
    e.channel,
    count(*) as cnt,
    max(e.discount) as max_discount,
    round(avg(e.shipping_fee), 2) as avg_shipping_fee
from valid e
left join stores d on e.store_id = d.id
group by d.city, e.channel;
