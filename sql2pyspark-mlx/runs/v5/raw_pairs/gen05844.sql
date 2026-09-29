select order_id, id from shipments intersect select order_id, id from shipments where order_id is not null
