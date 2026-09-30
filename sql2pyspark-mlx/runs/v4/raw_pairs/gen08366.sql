SELECT
    status,
    order_id,
    cost,
    WEEKOFYEAR(delivered_date) AS delivered_date_weekofyear
FROM shipments
WHERE cost <= 93
