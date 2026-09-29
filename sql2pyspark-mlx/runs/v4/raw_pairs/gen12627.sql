SELECT id, order_date, discount * amount discount_times_amount FROM orders WHERE channel = 'web' ORDER BY discount_times_amount DESC;
