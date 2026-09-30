select o.customer_id, o.discount, o.shipping_fee, s.city, s.sqft, s.region from orders o inner join stores s on s.id = o.store_id;
