select order_date from orders union all select end_date from subscriptions where start_date <= '2024-05-11' order by order_date desc;
