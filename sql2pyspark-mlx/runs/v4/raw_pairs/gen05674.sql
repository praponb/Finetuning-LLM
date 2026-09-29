select p.id, p.order_id, o.discount from payments p join orders o on o.id = p.order_id where p.amount is not null;
