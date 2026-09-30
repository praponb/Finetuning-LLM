select id, order_id, delivered_date, dense_rank() over (partition by order_id order by delivered_date) as dense_rnk from shipments where status in ('IN_TRANSIT', 'LOST');
