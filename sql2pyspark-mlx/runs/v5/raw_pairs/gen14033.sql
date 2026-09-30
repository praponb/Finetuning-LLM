WITH cleaned AS (
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        CAST(paid_at AS DATE) AS paid_at_day,
        CONCAT_WS('-', method, status) AS method_status_key,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM payments
    WHERE method IS NOT NULL
      AND status != 'SUCCESS'
)
SELECT
    a.id,
    a.order_id,
    a.paid_at,
    a.status,
    a.paid_at_day,
    d.shipping_fee,
    d.order_date,
    d.status AS order_status
FROM cleaned a
LEFT JOIN orders d
    ON a.order_id = d.id;
