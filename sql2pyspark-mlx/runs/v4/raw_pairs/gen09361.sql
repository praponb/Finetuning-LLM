select o.id, o.order_date, c.segment, s.sqft, s.city from orders o inner join customers c on o.customer_id = c.id left join stores s on o.store_id = s.id
