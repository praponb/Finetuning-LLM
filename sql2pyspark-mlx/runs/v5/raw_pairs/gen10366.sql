SELECT channel, SUM(shipping_fee) AS total_shipping_fee, COUNT(DISTINCT discount) AS unique_channels FROM orders WHERE channel <> 'mobile' OR amount > 720 GROUP BY channel;
