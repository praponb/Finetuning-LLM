select plan from subscriptions except all select plan from subscriptions where plan not in ('premium', 'standard')
