WITH cleaned AS (
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        MONTH(paid_at) AS paid_at_month,
        COALESCE(method, 'N/A') AS resolved_method
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-07-15 05:00:00'
      AND order_id IS NOT NULL
      AND method <> 'cash'
)
SELECT *
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at_month DESC, id) = 1;
