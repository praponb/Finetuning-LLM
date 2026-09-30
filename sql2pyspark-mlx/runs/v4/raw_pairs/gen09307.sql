SELECT id, shipping_fee, CONCAT_WS('/', channel, status) channel_status_key FROM orders WHERE status = 'CANCELLED' ORDER BY channel_status_key, id LIMIT 10;
