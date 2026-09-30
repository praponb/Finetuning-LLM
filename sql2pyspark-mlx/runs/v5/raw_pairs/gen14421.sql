select
    o.order_date,
    s.opened_date,
    s.sqft,
    s.region
from orders o
inner join stores s on o.store_id = s.id
where o.shipping_fee between 25 and 33
order by s.region desc;
