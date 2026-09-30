select shipped_date from shipments except select start_date shipped_date from subscriptions where end_date >= '2024-02-01';
