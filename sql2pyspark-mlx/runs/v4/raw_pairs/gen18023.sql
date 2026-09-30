SELECT id, browser, customer_id FROM sessions WHERE country IN ('JP', 'UK', 'DE') AND (customer_id IS NOT NULL OR customer_id IS NOT NULL) ORDER BY id DESC
