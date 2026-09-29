select order_date from orders where amount < 1030 union all select order_date from orders where discount <= 0.19
