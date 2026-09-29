SELECT started_at, device FROM sessions WHERE country = 'US' AND customer_id IS NULL AND device IN ('tablet', 'desktop', 'mobile');
