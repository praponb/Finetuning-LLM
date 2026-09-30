select id, amount from payments where (method in ('paypal', 'cash', 'bank_transfer') and method not in ('promptpay', 'card')) or to_date(paid_at) < '2025-10-01' order by amount;
