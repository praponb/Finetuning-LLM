select shipped_date from shipments where carrier in ('UPS', 'Flash') intersect select end_date from subscriptions where month(end_date) in (2, 4, 12)
