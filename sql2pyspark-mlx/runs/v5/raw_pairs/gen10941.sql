select id, order_id from shipments where year(delivered_date) >= 2024 union select id, order_id from shipments where carrier not in ('UPS', 'Flash')
