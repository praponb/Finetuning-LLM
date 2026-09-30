WITH cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        CONCAT(carrier, '/', status) carrier_status
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-07-18')
      AND order_id IS NOT NULL
)
SELECT
    DATE_TRUNC('MONTH', shipped_date) AS activity_month,
    COUNT_IF(carrier = 'DHL') AS dhl_events,
    COUNT(*) FILTER (WHERE carrier = 'Flash') AS flash_rows,
    SIZE(COLLECT_SET(carrier)) AS num_carriers
FROM cleaned
GROUP BY DATE_TRUNC('MONTH', shipped_date)
HAVING flash_rows > 1
ORDER BY activity_month DESC NULLS FIRST
