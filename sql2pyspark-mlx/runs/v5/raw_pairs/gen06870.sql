SELECT s.country, s.device, c.is_active, LOWER(c.segment) AS segment_lower FROM sessions s JOIN customers c ON s.customer_id = c.id WHERE s.device != 'desktop' ORDER BY device
