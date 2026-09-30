SELECT status, order_id, DAY(shipped_date) AS shipped_date_day
FROM shipments
WHERE delivered_date >= '2023-12-01'
