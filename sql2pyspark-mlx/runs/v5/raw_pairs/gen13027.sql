select id, shipped_date, order_id, carrier from shipments where carrier not in ('UPS', 'Kerry') and shipped_date <= '2024-04-01' and month(shipped_date) in (5, 8, 12)
