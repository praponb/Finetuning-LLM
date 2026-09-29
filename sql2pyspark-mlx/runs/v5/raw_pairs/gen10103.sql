WITH base AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-06-17')
),
filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        days_delivered_date_to_shipped_date,
        CASE WHEN carrier IN ('Flash', 'Kerry') THEN 'Tier 1' ELSE 'Other' END AS carrier_group
    FROM base
    WHERE shipped_date > DATE '2023-03-25'
),
ranked AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        days_delivered_date_to_shipped_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY days_delivered_date_to_shipped_date DESC NULLS FIRST, id) AS rank_in_group
        FROM filtered
    ) t
    WHERE rank_in_group = 1
),
valid AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        days_delivered_date_to_shipped_date,
        CAST(days_delivered_date_to_shipped_date AS DOUBLE) AS days_delivered_date_to_shipped_date_str
    FROM ranked
    WHERE days_delivered_date_to_shipped_date < 28
)
SELECT
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    ROUND(SUM(days_delivered_date_to_shipped_date) / NULLIF(COUNT(CASE WHEN carrier = 'UPS' THEN 1 END), 0), 2) AS days_delivered_date_to_shipped_date_per_ups,
    APPROX_PERCENTILE(days_delivered_date_to_shipped_date_str, ARRAY(0.25, 0.5, 0.75)) AS days_delivered_date_to_shipped_date_str_quartiles,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
FROM valid
GROUP BY DATE_TRUNC('YEAR', delivered_date);
