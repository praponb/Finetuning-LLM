select date_trunc('MONTH', o.order_date) as order_month, count(distinct p.amount) as unique_methods from payments p left join orders o on p.order_id = o.id group by date_trunc('MONTH', o.order_date);
