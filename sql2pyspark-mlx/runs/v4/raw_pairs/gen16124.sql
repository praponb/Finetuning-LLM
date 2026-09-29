select
    s.city,
    s.store_name,
    o.shipping_fee,
    o.discount,
    o.discount + o.shipping_fee as discount_plus_shipping_fee
from stores s
left join orders o on o.store_id = s.id
where s.sqft = 11530;
