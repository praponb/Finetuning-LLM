SELECT id, shipped_date, order_id, ROUND(cost, 0) AS cost_rounded FROM shipments WHERE delivered_date < '2023-12-17' OR carrier IN ('FedEx', 'DHL', 'Kerry') ORDER BY id DESC LIMIT 50;
