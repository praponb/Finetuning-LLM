select p.id, p.method, p.paid_at from payments p where p.order_id in (select id from orders where customer_id = 13) order by 2 desc;
