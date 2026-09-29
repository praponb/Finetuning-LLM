select id, order_id, delivered_date, cost, cost - lag(cost, 1) over (partition by order_id order by delivered_date, id) as cost_change from shipments
