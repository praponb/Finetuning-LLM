SELECT s.id, s.shipped_date, o.shipping_fee, CEIL(s.cost) AS cost_whole FROM shipments s JOIN orders o ON s.order_id = o.id WHERE s.status IN ('IN_TRANSIT', 'LOST') AND o.customer_id = 11;
