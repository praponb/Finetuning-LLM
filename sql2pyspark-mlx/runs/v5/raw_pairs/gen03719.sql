SELECT
    status,
    SUM(shipping_fee) AS total_shipping_fee,
    COUNT(*) AS cnt,
    ROUND(SUM(shipping_fee) * 100.0 / SUM(SUM(shipping_fee)) OVER (), 2) AS pct_of_total
FROM orders
GROUP BY status
