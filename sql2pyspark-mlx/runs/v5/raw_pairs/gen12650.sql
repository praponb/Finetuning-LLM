select o.amount, o.customer_id, o.shipping_fee, s.city, s.opened_date, s.sqft from orders o full outer join stores s on o.store_id = s.id where o.shipping_fee between 12 and 28 order by o.amount;
