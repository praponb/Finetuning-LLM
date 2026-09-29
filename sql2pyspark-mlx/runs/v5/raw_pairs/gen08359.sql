WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(carrier) AS carrier_lower,
        CASE WHEN cost >= 74 THEN 'High' WHEN cost >= 42 THEN 'Medium' ELSE 'Low' END AS cost_band,
        COALESCE(carrier, 'DIRECT') AS carrier_clean
    FROM shipments
    WHERE order_id IS NOT NULL
      AND cost > 62
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share,
        NTILE(4) OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_bucket,
        PERCENT_RANK() OVER w_grp AS cost_percent_rank
    FROM cleaned
    WHERE shipped_date > '2023-03-19'
    WINDOW w_grp AS (PARTITION BY carrier ORDER BY cost DESC NULLS LAST)
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.delivered_date,
        src.cost,
        src.carrier_lower,
        src.cost_band,
        src.carrier_clean,
        src.cost_share,
        src.cost_bucket,
        o.status AS order_status
    FROM scored src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(cost * 100, 2) AS cost_scaled,
        DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_date_label,
        COALESCE(order_status, 'UNKNOWN') AS resolved_order_status
    FROM joined
    WHERE carrier IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.delivered_date,
        a.cost,
        a.order_status,
        a.cost_scaled,
        a.delivered_date_label,
        b.channel
    FROM base a
    LEFT JOIN orders b
        ON a.order_id <=> b.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(order_status) AS clean_order_status,
        ROUND(cost_scaled, 0) AS cost_scaled_rounded
    FROM enriched
    WHERE delivered_date >= TO_DATE('2023-07-13')
      AND order_id IS NOT NULL
)
SELECT
    id,
    delivered_date,
    cost,
    delivered_date_label,
    channel,
    clean_order_status,
    cost_scaled_rounded,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'GROUP_A' ELSE 'CORE' END AS channel_zone
FROM valid
