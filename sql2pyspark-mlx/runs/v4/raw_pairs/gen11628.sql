select end_date from subscriptions where plan = 'standard' union select sale_date from store_sales where product_id in (2, 4, 9);
