select oi.order_id, p.launch_date from order_items oi full outer join products p on oi.product_id = p.id order by p.launch_date;
