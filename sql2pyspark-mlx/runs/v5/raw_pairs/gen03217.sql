select id, order_id, cost, min(cost) over (partition by order_id) as order_id_min_cost from shipments where cost between 27 and 58
