WITH filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        CONCAT(carrier, ' ', status) carrier_status,
        COALESCE(carrier, 'OTHER') carrier_clean
    FROM shipments
    WHERE delivered_date > DATE '2023-08-25'
      AND carrier IS NOT NULL
),
base AS (
    SELECT
        *,
        UPPER(carrier) carrier_upper,
        COALESCE(carrier, 'UNKNOWN') carrier_clean_2
    FROM filtered
    WHERE order_id IS NOT NULL
      AND carrier IS NOT DISTINCT FROM 'FedEx'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC NULLS FIRST, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
scored AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        carrier_status,
        carrier_clean,
        CUME_DIST() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS LAST) cost_cume_dist,
        ROW_NUMBER() OVER w_part row_seq
    FROM latest
    WHERE shipped_date <= '2025-07-01'
    WINDOW w_part AS (PARTITION BY carrier ORDER BY rank_in_group, id)
),
with_windows AS (
    SELECT
        *,
        MIN(cost_cume_dist) OVER (PARTITION BY carrier, order_id) carrier_min_cost_cume_dist,
        NTILE(10) OVER (PARTITION BY carrier ORDER BY cost DESC, id) cost_bucket
    FROM scored
)
SELECT
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count,
    COUNT(DISTINCT carrier) AS distinct_carriers,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(CASE WHEN carrier = 'UPS' THEN 1 END) AS ups_count
FROM with_windows
GROUP BY DATE_TRUNC('MONTH', delivered_date)
HAVING COUNT(DISTINCT id) >= 1
ORDER BY activity_month NULLS LAST
