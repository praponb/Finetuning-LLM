SELECT *, TRIM(status) AS clean_status, INITCAP(TRIM(channel)) AS channel_title FROM orders WHERE shipping_fee <= 35;
