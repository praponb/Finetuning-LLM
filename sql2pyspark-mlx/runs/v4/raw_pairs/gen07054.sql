select p.id, p.order_id, o.amount, s.sqft, s.region from payments p inner join orders o on p.order_id = o.id inner join stores s on o.store_id = s.id order by s.sqft desc;
