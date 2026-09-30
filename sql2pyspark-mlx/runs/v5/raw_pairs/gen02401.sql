SELECT id, order_id, cost, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost DESC) cost_dense_rank FROM shipments ORDER BY id DESC;
