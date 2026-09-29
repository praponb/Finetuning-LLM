SELECT * FROM shipments WHERE MONTH(shipped_date) IN (2, 6, 12) AND (carrier IN ('FedEx', 'Kerry', 'DHL') OR order_id IN (7, 35)) ORDER BY delivered_date DESC, carrier
