select p.id, p.method from payments p where p.order_id not in (select id from orders where order_date <= '2023-05-01') order by p.id desc, method;
