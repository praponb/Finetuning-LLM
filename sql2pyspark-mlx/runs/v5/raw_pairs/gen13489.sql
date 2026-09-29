select s.shipped_date, s.order_id, o.amount, o.customer_id from shipments s left outer join orders o on s.order_id = o.id where o.shipping_fee between 17 and 37;
