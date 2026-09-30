WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        CASE WHEN cost >= 89 THEN 'High' WHEN cost >= 19 THEN 'Medium' ELSE 'Low' END AS cost_band,
        CASE WHEN status IN ('RETURNED', 'LOST') THEN 'Priority' ELSE 'Other' END AS status_group
    FROM shipments
    WHERE carrier IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, carrier
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id, carrier
    FROM shipments
    WHERE shipped_date BETWEEN '2023-09-01' AND '2025-07-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id AND s.carrier = k.carrier
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.carrier,
        s.delivered_date,
        s.cost_band,
        d.shipping_fee,
        d.order_date,
        d.status
    FROM reconciled s
    LEFT JOIN orders d
        ON s.order_id <=> d.id
    WHERE d.status IN ('COMPLETED', 'PENDING', 'CANCELLED')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        delivered_date,
        cost_band,
        shipping_fee,
        order_date,
        COUNT(shipping_fee) OVER (PARTITION BY carrier) AS carrier_count_shipping_fee,
        MAX(shipping_fee) OVER (PARTITION BY status, order_id) AS status_max_shipping_fee,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY shipping_fee DESC) AS shipping_fee_percent_rank
    FROM enriched
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, shipping_fee_percent_rank + status_max_shipping_fee AS shipping_fee_percent_rank_plus_status_max_shipping_fee
    FROM metrics
    WHERE delivered_date >= TO_DATE('2023-06-18')
      AND carrier_count_shipping_fee BETWEEN 12 AND 15
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', order_date) AS week_start,
        DATE_TRUNC('MONTH', delivered_date) AS activity_month,
        COUNT(*) AS event_count,
        ROUND(AVG(shipping_fee_percent_rank), 2) AS avg_shipping_fee_percent_rank,
        MAX(shipping_fee) AS max_shipping_fee,
        MIN(shipping_fee_percent_rank_plus_status_max_shipping_fee) AS min_shipping_fee_percent_rank_plus_status_max_shipping_fee,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', order_date) ORDER BY MIN(shipping_fee_percent_rank_plus_status_max_shipping_fee) DESC) AS min_shipping_fee_percent_rank_plus_status_max_shipping_fee_dense_rank
    FROM filtered
    GROUP BY DATE_TRUNC('WEEK', order_date), DATE_TRUNC('MONTH', delivered_date)
)
SELECT
    week_start,
    activity_month,
    avg_shipping_fee_percent_rank,
    max_shipping_fee,
    min_shipping_fee_percent_rank_plus_status_max_shipping_fee_dense_rank
FROM stats
ORDER BY max_shipping_fee DESC NULLS FIRST
