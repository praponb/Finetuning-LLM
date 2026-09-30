SELECT
    CASE WHEN amount >= 1550 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    CASE WHEN shipping_fee >= 44 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_tier,
    AVG(amount) FILTER (WHERE status = 'RETURNED') AS returned_avg_amount,
    CORR(discount, shipping_fee) AS corr_discount_shipping_fee,
    SUM(amount) FILTER (WHERE channel = 'mobile') AS mobile_total_amount
FROM orders
GROUP BY CASE WHEN amount >= 1550 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN shipping_fee >= 44 THEN 'HIGH' ELSE 'LOW' END
ORDER BY returned_avg_amount ASC;
