SELECT id, email, COALESCE(segment, 'N/A') AS segment_filled
FROM customers
ORDER BY email, segment_filled DESC
