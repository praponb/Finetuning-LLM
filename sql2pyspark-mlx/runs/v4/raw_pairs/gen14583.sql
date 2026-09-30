SELECT
    status,
    STDDEV_SAMP(shipping_fee) AS shipping_fee_stddev,
    STDDEV_SAMP(amount) AS amount_stddev,
    AVG(shipping_fee) FILTER (WHERE channel = 'mobile') AS mobile_avg_shipping_fee
FROM orders
GROUP BY status
HAVING STDDEV_SAMP(amount) > 0;
