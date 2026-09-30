WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        status,
        DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date,
        INITCAP(TRIM(carrier)) AS carrier_title
    FROM shipments
    WHERE order_id IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(shipped_date) AS shipped_date_month_end
    FROM cleaned
    WHERE shipped_date >= DATE '2023-03-07'
      AND order_id IS NOT NULL
      AND order_id = 5
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY days_delivered_date_to_shipped_date NULLS FIRST) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        shipped_date,
        status,
        days_delivered_date_to_shipped_date,
        carrier_title,
        shipped_date_month_end,
        dedup_rank,
        PERCENT_RANK() OVER w_hist AS days_delivered_date_to_shipped_date_percent_rank,
        LEAD(days_delivered_date_to_shipped_date, 1) OVER (PARTITION BY status ORDER BY shipped_date, id) AS next_days_delivered_date_to_shipped_date
    FROM latest
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY days_delivered_date_to_shipped_date NULLS LAST)
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id, status
    FROM metrics
    EXCEPT DISTINCT
    SELECT order_id, status
    FROM shipments
    WHERE delivered_date BETWEEN '2024-10-01' AND '2025-06-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.order_id = k.order_id AND s.status = k.status
),
with_dims AS (
    -- Attach orders attributes
    SELECT s.*, o.channel
    FROM reconciled s
    LEFT JOIN orders o
        ON s.order_id = o.id
)
SELECT
    id,
    shipped_date,
    status,
    days_delivered_date_to_shipped_date,
    shipped_date_month_end,
    dedup_rank,
    channel
FROM with_dims;
