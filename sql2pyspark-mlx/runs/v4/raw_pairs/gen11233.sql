WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(carrier)) AS carrier_title
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-03-05')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS(',', status, carrier) AS status_carrier_key
    FROM valid
    WHERE shipped_date > DATE '2023-05-24'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS row_seq,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        LEAD(cost, 1) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS next_cost
    FROM cleaned
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN carrier = 'DHL' THEN 1 END), 0), 2) AS cost_per_dhl,
    COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count
FROM enriched
GROUP BY DATE_TRUNC('QUARTER', delivered_date)
HAVING fedex_count >= 1
