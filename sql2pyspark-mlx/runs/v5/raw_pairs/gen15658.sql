select status, min(paid_at) as min_paid_at from payments where amount > 880 or method in ('paypal', 'bank_transfer') group by status order by status;
