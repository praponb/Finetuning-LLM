SELECT
    id,
    order_id,
    method,
    status,
    TRIM(method) AS clean_method,
    CASE WHEN amount >= 1710 THEN 'High' WHEN amount >= 350 THEN 'Medium' ELSE 'Low' END AS amount_band
FROM payments
WHERE order_id IS NULL AND (method IN ('bank_transfer', 'paypal') OR order_id IS NULL)
