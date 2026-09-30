SELECT channel, COUNT(*) row_count, MAX(amount) max_amount, MAX(shipping_fee) - MIN(shipping_fee) shipping_fee_range FROM orders WHERE amount IS NOT NULL GROUP BY channel ORDER BY channel LIMIT 10;
