select *, coalesce(amount, 0) as amount_filled, datediff(current_date(), paid_at) as days_since_paid_at from payments where order_id is not null order by amount
