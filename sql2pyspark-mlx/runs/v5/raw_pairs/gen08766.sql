select order_id, min(delivered_date) as min_delivered_date, max(cost) - min(cost) as cost_range, max(delivered_date) as last_delivered_date from shipments group by order_id having cost_range > 56;
