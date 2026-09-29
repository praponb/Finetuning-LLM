WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date,
        SUBSTRING(status, 1, 2) AS status_prefix,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM shipments
    WHERE shipped_date >= DATE '2023-07-21'
      AND order_id IS NOT NULL
      AND shipped_date BETWEEN '2025-04-01' AND '2025-05-16'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_ADD(shipped_date, 38) AS shipped_date_plus_38d,
        CASE WHEN status IN ('LOST', 'RETURNED') THEN 'Tier 1' ELSE 'Other' END AS status_group,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier_2
    FROM cleaned
    WHERE shipped_date > TO_DATE('2024-03-22')
      AND order_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.amount,
        d.channel,
        d.discount
    FROM filtered src
    JOIN orders d
        ON src.order_id = d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id ASC) = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM deduped a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.shipped_date_plus_38d BETWEEN pr.starts_on AND pr.ends_on
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN discount_pct >= 0.29 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    CASE WHEN days_shipped_date_to_delivered_date >= 69 THEN 'HIGH' ELSE 'LOW' END AS days_shipped_date_to_delivered_date_tier,
    COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END) AS spring10_count,
    COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
    COUNT(*) AS cnt,
    GROUPING(CASE WHEN discount_pct >= 0.29 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM attributed
GROUP BY GROUPING SETS ((CASE WHEN discount_pct >= 0.29 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN days_shipped_date_to_delivered_date >= 69 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN discount_pct >= 0.29 THEN 'HIGH' ELSE 'LOW' END));
