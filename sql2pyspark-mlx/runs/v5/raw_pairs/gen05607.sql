WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        DATE_FORMAT(delivered_date, 'yyyy-MM') delivered_date_label,
        COALESCE(carrier, 'UNKNOWN') carrier_clean
    FROM shipments
    WHERE carrier IN ('DHL', 'Kerry')
      AND carrier IS NOT DISTINCT FROM 'UPS'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DAY(delivered_date) delivered_date_day,
        COALESCE(carrier, 'UNKNOWN') carrier_filled
    FROM cleaned
    WHERE carrier IS NOT NULL
      AND status = 'RETURNED'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC NULLS LAST, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
filtered_shipments AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(status) status_lower
    FROM ranked
    WHERE delivered_date >= TO_DATE('2023-07-08')
      AND status IN ('RETURNED', 'IN_TRANSIT')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        row_num / NULLIF(delivered_date_day, 0) row_num_per_delivered_date_day,
        row_num * delivered_date_day row_num_times_delivered_date_day,
        COALESCE(carrier, 'OTHER') carrier_filled_2
    FROM filtered_shipments
    WHERE status NOT IN ('RETURNED', 'DELIVERED')
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    carrier,
    metric,
    reading
FROM (
    SELECT
        id,
        carrier,
        row_num_per_delivered_date_day,
        row_num_times_delivered_date_day
    FROM valid
) src
UNPIVOT (
    reading FOR metric IN (row_num_per_delivered_date_day, row_num_times_delivered_date_day)
) u
