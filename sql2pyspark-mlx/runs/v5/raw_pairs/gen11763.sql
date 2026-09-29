SELECT id, order_date, customer_id, store_id, LOWER(channel) channel_lower, amount * discount amount_times_discount FROM orders WHERE discount <= 0.21;
