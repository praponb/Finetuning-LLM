select p.id, p.amount, p.order_id from payments p where p.amount >= 1110 and p.order_id not in (select id from orders where amount <= 440) order by p.order_id desc;
