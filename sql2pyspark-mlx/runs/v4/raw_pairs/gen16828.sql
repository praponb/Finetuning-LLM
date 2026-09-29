SELECT id, amount, order_date, amount / NULLIF(shipping_fee, 0) amount_per_shipping_fee FROM orders WHERE order_date BETWEEN '2025-06-01' AND '2025-10-01';
