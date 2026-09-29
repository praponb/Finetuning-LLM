select id, order_date, coalesce(amount, 0) as amount_filled from orders
