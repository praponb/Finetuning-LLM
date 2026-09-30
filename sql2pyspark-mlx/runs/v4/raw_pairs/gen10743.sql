select o.id, o.order_date, s.sqft, s.city, c.country, c.credit_limit from orders o left join stores s on s.id = o.store_id right join customers c on o.customer_id = c.id
