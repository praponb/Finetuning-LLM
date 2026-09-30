select id, status, coalesce(cost, 0) as cost_filled from shipments where order_id = 39;
