WITH ranked AS (
    SELECT *
    FROM shipments
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY shipped_date DESC NULLS FIRST) = 1
),
latest AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) = 1
),
base AS (
    SELECT
        *,
        DATE_TRUNC('MONTH', shipped_date) shipped_date_period,
        COALESCE(carrier, 'OTHER') carrier_filled
    FROM latest
    WHERE delivered_date > TO_DATE('2024-04-05')
      AND order_id IS NOT NULL
)
SELECT
    *,
    NTILE(4) OVER (PARTITION BY order_id ORDER BY cost, id) cost_bucket,
    PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost DESC NULLS LAST) cost_percent_rank,
    DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost) cost_dense_rank,
    DENSE_RANK() OVER (ORDER BY cost DESC) cost_dense_rank_2
FROM base
