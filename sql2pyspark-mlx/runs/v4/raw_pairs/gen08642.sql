select s.id, s.delivered_date, o.amount, o.customer_id, c.country from shipments s inner join orders o on s.order_id = o.id inner join customers c on o.customer_id = c.id;
