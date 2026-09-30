WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        cost,
        status,
        LEAD(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS next_cost,
        LAG(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS prev_cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS seq_no
    FROM shipments
    WHERE delivered_date BETWEEN '2024-03-01' AND '2025-03-01'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        seq_no * next_cost AS seq_no_times_next_cost,
        CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END AS is_kerry,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM scored
    WHERE carrier NOT IN ('DHL', 'FedEx', 'Flash')
      AND carrier IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY is_kerry DESC NULLS FIRST, id) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        status,
        next_cost,
        prev_cost,
        is_kerry,
        resolved_carrier,
        CAST(seq_no AS STRING) AS seq_no_str,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM ranked
    WHERE carrier IS NOT NULL
),
ranked_shipments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        carrier,
        status,
        prev_cost,
        is_kerry,
        resolved_carrier,
        seq_no_str,
        carrier_clean
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY is_kerry DESC, id) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked_shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY is_kerry DESC NULLS FIRST, id) = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        status,
        CASE WHEN is_kerry >= 60 THEN 'LARGE' ELSE 'SMALL' END AS is_kerry_tier,
        COUNT(*) AS event_count,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
        COUNT(*) FILTER (WHERE carrier = 'DHL') AS dhl_rows,
        AVG(prev_cost) FILTER (WHERE status = 'DELIVERED') AS delivered_avg_prev_cost,
        SUM(CASE WHEN status = 'DELIVERED' THEN is_kerry ELSE 0 END) AS delivered_is_kerry
    FROM deduped
    GROUP BY GROUPING SETS ((status, CASE WHEN is_kerry >= 60 THEN 'LARGE' ELSE 'SMALL' END), (status))
)
SELECT
    status,
    is_kerry_tier,
    event_count,
    carrier_list,
    dhl_rows,
    delivered_avg_prev_cost,
    delivered_is_kerry,
    CASE WHEN status RLIKE '^(IN_TRANSIT|DELIVERED)$' THEN 'GROUP_B' WHEN status RLIKE '^(LOST|RETURNED)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS status_zone
FROM stats
ORDER BY status_zone DESC;
