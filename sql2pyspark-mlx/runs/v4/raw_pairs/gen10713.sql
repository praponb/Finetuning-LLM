select method, max(paid_at) as latest_paid_at, min(amount) as min_amount from payments where status in ('SUCCESS', 'FAILED') group by method
