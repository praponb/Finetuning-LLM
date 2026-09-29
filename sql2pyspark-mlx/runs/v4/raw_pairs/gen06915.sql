select * from (select carrier, status, cost from shipments where carrier = 'Flash') pivot (max(cost) for status in ('RETURNED', 'DELIVERED', 'IN_TRANSIT', 'LOST'))
