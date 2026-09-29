SELECT
    CASE WHEN amount >= 1520 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    CASE WHEN shipping_fee >= 36 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_tier,
    MAX(amount) FILTER (WHERE status = 'PENDING') AS pending_max_amount,
    MAX(amount) FILTER (WHERE channel = 'mobile') AS mobile_max_amount,
    COUNT_IF(status = 'RETURNED') AS returned_events,
    COUNT(*) FILTER (WHERE status = 'SHIPPED') AS shipped_rows,
    CORR(amount, discount) AS corr_amount_discount,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN amount >= 1520 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT_IF(status = 'RETURNED') DESC) AS returned_events_dense_rank
FROM orders
GROUP BY CASE WHEN amount >= 1520 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN shipping_fee >= 36 THEN 'LARGE' ELSE 'SMALL' END
HAVING MAX(amount) FILTER (WHERE channel = 'mobile') > 0
ORDER BY shipping_fee_tier DESC
