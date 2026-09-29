WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        MONTH(delivered_date) AS delivered_date_month,
        COALESCE(carrier, 'Unknown') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(cost, TRUE) OVER (ORDER BY delivered_date_month ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        NTILE(5) OVER (PARTITION BY order_id, carrier ORDER BY delivered_date_month ASC, id ASC) AS delivered_date_month_bucket,
        AVG(delivered_date_month) OVER (PARTITION BY status ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS delivered_date_month_avg_5
    FROM cleaned
),
cleaned_shipments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        delivered_date_month,
        carrier_filled,
        first_known_cost,
        delivered_date_month_bucket,
        delivered_date_month_avg_5,
        DATE_FORMAT(delivered_date, 'MMM yyyy') AS delivered_date_label,
        COALESCE(carrier, 'OTHER') AS carrier_filled_2
    FROM with_windows
    WHERE delivered_date > DATE '2024-04-16'
      AND carrier IS NOT NULL
      AND delivered_date_month_avg_5 <= 55
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    order_id,
    AVG(delivered_date_month) FILTER (WHERE carrier = 'UPS') AS ups_avg_delivered_date_month,
    SUM(CASE WHEN carrier = 'Kerry' THEN delivered_date_month ELSE 0 END) AS kerry_delivered_date_month,
    COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count
FROM cleaned_shipments
GROUP BY year_start, order_id;
