SELECT *
FROM shipments
WHERE carrier IN ('FedEx', 'Kerry') AND status IN ('RETURNED', 'DELIVERED', 'IN_TRANSIT') AND carrier = 'Kerry';
