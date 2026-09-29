select o.channel, o.status, round(avg(p.amount), 2) as avg_amount from payments p inner join orders o on p.order_id = o.id where o.customer_id in (4, 18) group by o.channel, o.status;
