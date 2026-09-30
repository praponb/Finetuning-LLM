select start_date from subscriptions where status in ('ACTIVE', 'EXPIRED', 'CANCELLED') union all select sale_date from store_sales
