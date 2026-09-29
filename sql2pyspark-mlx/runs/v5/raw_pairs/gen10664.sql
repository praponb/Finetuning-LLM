WITH base AS (
    SELECT
        id,
        shipped_date,
        cost,
        status,
        DATE_TRUNC('week', delivered_date) AS delivered_date_period
    FROM shipments
    WHERE order_id IS NOT NULL
),
kept_keys AS (
    SELECT status, id
    FROM base
    INTERSECT ALL
    SELECT status, id
    FROM shipments
    WHERE carrier != 'Flash'
),
reconciled AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
valid AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), delivered_date_period) AS days_since_delivered_date_period,
        ADD_MONTHS(shipped_date, 6) AS shipped_date_plus_months
    FROM reconciled
    WHERE shipped_date > DATE '2023-06-20'
),
with_windows AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY cost DESC) AS cost_dense_rank
    FROM valid
    WHERE HOUR(delivered_date_period) BETWEEN 9 AND 16
)
SELECT
    id,
    shipped_date,
    cost,
    status,
    days_since_delivered_date_period,
    shipped_date_plus_months,
    cost_dense_rank
FROM with_windows
ORDER BY days_since_delivered_date_period DESC
