WITH base AS (
    SELECT *
    FROM shipments
    WHERE (status IN ('LOST', 'DELIVERED') AND carrier IN ('FedEx', 'UPS', 'DHL')) OR status != 'DELIVERED'
),
shipped_month_summary AS (
    SELECT MONTH(shipped_date) shipped_month, COUNT(*) num_shipments
    FROM base
    GROUP BY MONTH(shipped_date)
)
SELECT *
FROM shipped_month_summary
WHERE num_shipments > 7
ORDER BY shipped_month;
