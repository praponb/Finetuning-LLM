SELECT id, channel, CONCAT_WS('/', status, channel) AS status_channel_key FROM orders WHERE store_id IS NULL;
