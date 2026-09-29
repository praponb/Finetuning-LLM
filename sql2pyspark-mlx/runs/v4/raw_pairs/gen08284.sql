SELECT
    id,
    status,
    amount,
    paid_at,
    CASE WHEN amount >= 1250 THEN 'High' WHEN amount >= 630 THEN 'Medium' ELSE 'Low' END AS amount_tier
FROM payments
WHERE order_id IN (3, 27) AND status IN ('REFUNDED', 'FAILED')
